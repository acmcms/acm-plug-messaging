/*
 * Created on 09.10.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.al.api.messaging;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import ru.myx.ae1.BaseRT3;
import ru.myx.ae1.access.AccessGroup;
import ru.myx.ae1.access.AccessManager;
import ru.myx.ae1.access.AccessUser;
import ru.myx.ae1.messaging.MessageFactory;
import ru.myx.ae1.messaging.Messaging;
import ru.myx.ae3.Engine;
import ru.myx.ae3.access.AccessPrincipal;
import ru.myx.ae3.act.Act;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.email.Email;
import ru.myx.ae3.email.EmailSender;
import ru.myx.ae3.report.Report;
import ru.myx.jdbc.lock.Runner;

/**
 * @author myx
 * 		
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class MessagingRunner implements Runner, Runnable {
	
	private final static void discardQueue(final Connection conn, final int luid) throws SQLException {
		
		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM m1Queue WHERE msgLuid=?")) {
			ps.setInt(1, luid);
			ps.execute();
		}
	}
	
	private final static void doSendLocal(final Connection conn, final MessageImpl info) throws SQLException {
		
		final String userId = info.msgTarget.substring("#local: ".length());
		try (final PreparedStatement ps = conn
				.prepareStatement("INSERT INTO m1Inbox(msgId,msgUserId,msgPriority,msgRead,msgTarget) SELECT msgId,?,msgPriority,?,msgTarget FROM m1Queue WHERE msgLuid=?")) {
			ps.setString(1, userId);
			ps.setString(2, "N");
			ps.setInt(3, info.msgLuid);
			ps.execute();
		}
		MessagingRunner.discardQueue(conn, info.msgLuid);
	}
	
	private final Plugin parent;
	
	private boolean started = false;
	
	MessagingRunner(final Plugin parent) {
		this.parent = parent;
	}
	
	private final void doExtractAccess(final Connection conn, final MessageImpl info) throws SQLException {
		
		final String accessDescription = info.msgTarget.substring("#access: ".length());
		final int accessPosition = accessDescription.indexOf(',');
		if (accessPosition != -1) {
			final String permission = accessDescription.substring(0, accessPosition);
			final String path = accessDescription.substring(accessPosition + 1);
			final AccessManager manager = this.parent.getServer().getAccessManager();
			final AccessPrincipal<?>[] principals = manager.securityGetAccessEffective(path, permission);
			if (principals != null && principals.length > 0) {
				final Set<String> targets = this.doFillTargets(manager, principals, new TreeSet<String>(), info.msgInteractive);
				try (final PreparedStatement ps = conn.prepareStatement(
						"INSERT INTO m1Queue(msgId,msgQueued,msgExpire,msgFailCounter,msgPriority,msgInteractive,msgTarget) SELECT msgId,msgQueued,msgExpire,msgFailCounter,msgPriority,msgInteractive,? FROM m1Queue WHERE msgLuid=?")) {
					for (final String target : targets) {
						ps.setString(1, target);
						ps.setInt(2, info.msgLuid);
						ps.execute();
						ps.clearParameters();
					}
				}
			}
		}
		MessagingRunner.discardQueue(conn, info.msgLuid);
	}
	
	private final void doExtractGroup(final Connection conn, final MessageImpl info) throws SQLException {
		
		final String groupId = info.msgTarget.substring("#group: ".length());
		final AccessManager manager = this.parent.getServer().getAccessManager();
		final AccessGroup<?> group = manager.getGroup(groupId, false);
		if (group != null) {
			final AccessUser<?>[] users = manager.getUsers(group);
			if (users != null && users.length > 0) {
				try (final PreparedStatement ps = conn.prepareStatement(
						"INSERT INTO m1Queue(msgId,msgQueued,msgExpire,msgFailCounter,msgPriority,msgInteractive,msgTarget) SELECT msgId,msgQueued,msgExpire,msgFailCounter,msgPriority,msgInteractive,? FROM m1Queue WHERE msgLuid=?")) {
					if (info.msgInteractive) {
						for (int i = users.length - 1; i >= 0; --i) {
							final AccessUser<?> user = users[i];
							if (user != null) {
								ps.setString(1, "#local: " + user.getKey());
								ps.setInt(2, info.msgLuid);
								ps.execute();
								ps.clearParameters();
							}
						}
					} else {
						for (int i = users.length - 1; i >= 0; --i) {
							final AccessUser<?> user = users[i];
							if (user != null) {
								final String email = user.getEmail();
								if (email != null && email.trim().length() > 0) {
									ps.setString(1, "#email: " + email);
									ps.setInt(2, info.msgLuid);
									ps.execute();
									ps.clearParameters();
								} else {
									ps.setString(1, "#local: " + user.getKey());
									ps.setInt(2, info.msgLuid);
									ps.execute();
									ps.clearParameters();
								}
							}
						}
					}
				}
			}
		}
		MessagingRunner.discardQueue(conn, info.msgLuid);
	}
	
	private final Set<String> doFillTargets(final AccessManager manager, final AccessPrincipal<?>[] principals, final Set<String> targets, final boolean interactive) {
		
		if (interactive) {
			for (int i = principals.length - 1; i >= 0; --i) {
				final AccessPrincipal<?> principal = principals[i];
				if (principal != null) {
					if (principal.isPerson()) {
						targets.add("#local: " + principal.getKey());
					} else {
						final AccessUser<?>[] users = manager.getUsers((AccessGroup<?>) principal);
						if (users != null && users.length > 0) {
							this.doFillTargets(manager, users, targets, interactive);
						}
					}
				}
			}
		} else {
			for (int i = principals.length - 1; i >= 0; --i) {
				final AccessPrincipal<?> principal = principals[i];
				if (principal != null) {
					if (principal.isPerson()) {
						final AccessUser<?> user = (AccessUser<?>) principal;
						final String email = user.getEmail();
						if (email != null && email.trim().length() > 0) {
							targets.add("#email: " + email);
						} else {
							targets.add("#local: " + user.getKey());
						}
					} else {
						final AccessUser<?>[] users = manager.getUsers((AccessGroup<?>) principal);
						if (users != null && users.length > 0) {
							this.doFillTargets(manager, users, targets, interactive);
						}
					}
				}
			}
		}
		return targets;
	}
	
	private final void doSendUser(final Connection conn, final MessageImpl info) throws SQLException {
		
		if (info.msgInteractive) {
			MessagingRunner.doSendLocal(conn, info);
		} else {
			final String userId = info.msgTarget.substring("#uid: ".length());
			final AccessManager manager = this.parent.getServer().getAccessManager();
			final AccessUser<?> user = manager.getUser(userId, false);
			if (user != null) {
				final String email = user.getEmail();
				if (email != null && email.trim().length() > 0) {
					try (final PreparedStatement ps = conn.prepareStatement(
							"INSERT INTO m1Queue(msgId,msgQueued,msgExpire,msgFailCounter,msgPriority,msgInteractive,msgTarget) SELECT msgId,msgQueued,msgExpire,msgFailCounter,msgPriority,msgInteractive,? FROM m1Queue WHERE msgLuid=?")) {
						ps.setString(1, "#email: " + email);
						ps.setInt(2, info.msgLuid);
						ps.execute();
					}
					MessagingRunner.discardQueue(conn, info.msgLuid);
				} else {
					MessagingRunner.doSendLocal(conn, info);
				}
			} else {
				MessagingRunner.discardQueue(conn, info.msgLuid);
			}
		}
	}
	
	private final void doStartExternal(final LinkedList<?> sendExternal, final LinkedList<MessageImpl> doneExternal) {
		
		final EmailSender sender = BaseRT3.runtime().getEmailSender();
		for (;;) {
			final Object next;
			synchronized (doneExternal) {
				if (sendExternal.isEmpty()) {
					break;
				}
				next = sendExternal.getFirst();
			}
			final MessageImpl info = (MessageImpl) next;
			final Email data = new Email();
			{
				final MessageFactory factory = Messaging.getMessageFactory(info.fcId);
				if (factory == null) {
					data.put("Subject", "No factory: " + info.fcId);
					data.put("Body", "No factory: " + info.fcId);
				} else {
					final BaseObject external = factory.createExternalMessage(info);
					if (external != null) {
						data.baseDefineImportAllEnumerable(external);
					}
				}
			}
			{
				final AccessUser<?> user = this.parent.getServer().getAccessManager().getUser(info.msgOwnerId, true);
				if (user == null) {
					data.put("From", "uid." + info.msgOwnerId + "@" + this.parent.getServer().getDomainId());
				} else {
					final String login = user.getLogin();
					final String email = user.getEmail();
					if (email == null || email.trim().length() == 0) {
						if (login == null || login.length() == 0) {
							data.put("From", "uid." + info.msgOwnerId + "@" + this.parent.getServer().getDomainId());
						} else {
							data.put("From", login + "@" + this.parent.getServer().getDomainId());
						}
					} else {
						if (login == null || login.length() == 0 || email.startsWith(login)) {
							data.put("From", email);
						} else {
							data.put("From", '"' + login + "\" <" + email + ">");
						}
					}
				}
			}
			data.put("To", info.msgTarget.substring("#email: ".length()));
			sender.sendEmail(data);
			synchronized (doneExternal) {
				doneExternal.addLast(info);
				sendExternal.removeFirst();
			}
		}
	}
	
	@Override
	public int getVersion() {
		
		return 1;
	}
	
	@Override
	public void run() {
		
		if (!this.started) {
			return;
		}
		try (final Connection conn = this.parent.nextConnection()) {
			if (conn == null) {
				if (this.started) {
					Act.later(null, this, 15000L);
				}
				return;
			}
			try {
				final List<MessageImpl> pending;
				try (final PreparedStatement ps = conn.prepareStatement(
						"SELECT q.msgLuid,q.msgId,m.msgDate,q.msgInteractive,q.msgTarget,m.msgOwnerId,m.fcId FROM m1Queue q, m1Messages m WHERE q.msgQueued<? AND q.msgFailCounter>0 AND q.msgId=m.msgId ORDER BY q.msgPriority DESC, q.msgQueued ASC",
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY)) {
					ps.setMaxRows(50);
					ps.setTimestamp(1, new Timestamp(Engine.fastTime()));
					try (final ResultSet rs = ps.executeQuery()) {
						if (rs.next()) {
							pending = new ArrayList<>();
							do {
								pending.add(
										new MessageImpl(
												this.parent.getManager(),
												rs.getInt(1),
												rs.getString(2),
												rs.getTimestamp(3).getTime(),
												"Y".equals(rs.getString(4)),
												false,
												rs.getString(5),
												rs.getString(6),
												rs.getString(7)));
							} while (rs.next());
						} else {
							pending = null;
						}
					}
				}
				if (pending == null) {
					return;
				}
				Report.info("MMAN", "Queue pending: " + pending);
				final LinkedList<MessageImpl> queueExternal;
				final LinkedList<MessageImpl> sendExternal = new LinkedList<>();
				{
					for (final Iterator<MessageImpl> i = pending.iterator(); i.hasNext();) {
						final MessageImpl info = i.next();
						if (info.msgTarget.startsWith("#email: ")) {
							sendExternal.add(info);
							i.remove();
						}
					}
					if (sendExternal.isEmpty()) {
						queueExternal = null;
					} else {
						queueExternal = new LinkedList<>();
					}
				}
				conn.setAutoCommit(false);
				if (queueExternal != null) {
					this.doStartExternal(sendExternal, queueExternal);
				}
				if (!pending.isEmpty()) {
					for (final MessageImpl info : pending) {
						try {
							if (info.msgTarget.startsWith("#uid: ")) {
								this.doSendUser(conn, info);
							} else if (info.msgTarget.startsWith("#local: ")) {
								MessagingRunner.doSendLocal(conn, info);
							} else if (info.msgTarget.startsWith("#group: ")) {
								this.doExtractGroup(conn, info);
							} else if (info.msgTarget.startsWith("#access: ")) {
								this.doExtractAccess(conn, info);
							} else {
								Report.warning("MMAN", "Unknown target type: " + info.msgTarget);
							}
							conn.commit();
							if (queueExternal != null) {
								synchronized (queueExternal) {
									if (!queueExternal.isEmpty()) {
										while (!queueExternal.isEmpty()) {
											final Object result = queueExternal.removeFirst();
											if (result != null) {
												final MessageImpl sent = (MessageImpl) result;
												MessagingRunner.discardQueue(conn, sent.msgLuid);
											}
										}
										conn.commit();
									}
								}
							}
						} catch (final SQLException e) {
							Report.exception("MMAN", "while exhausting queue", e);
							conn.rollback();
						}
					}
				}
				if (queueExternal != null) {
					for (;;) {
						synchronized (queueExternal) {
							if (!queueExternal.isEmpty()) {
								while (!queueExternal.isEmpty()) {
									final Object result = queueExternal.removeFirst();
									if (result != null) {
										final MessageImpl sent = (MessageImpl) result;
										MessagingRunner.discardQueue(conn, sent.msgLuid);
									}
								}
								conn.commit();
							}
							if (sendExternal.isEmpty()) {
								break;
							}
							try {
								Thread.sleep(100L);
							} catch (final InterruptedException e) {
								return;
							}
						}
					}
				}
			} finally {
				if (this.started) {
					Act.later(null, this, 30000L);
				}
			}
		} catch (final SQLException e) {
			Report.exception("MMAN", "Looking for queue", e);
		}
	}
	
	@Override
	public void start() {
		
		if (!this.started) {
			synchronized (this) {
				if (!this.started) {
					Act.later(null, this, 10000L);
					this.started = true;
				}
			}
		}
	}
	
	@Override
	public void stop() {
		
		this.started = false;
	}
	
	@Override
	public String toString() {
		
		return "MRunner: parent=" + this.parent;
	}
}
