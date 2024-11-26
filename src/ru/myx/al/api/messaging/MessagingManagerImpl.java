/*
 * Created on 09.10.2004
 *
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.al.api.messaging;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import ru.myx.ae1.messaging.Message;
import ru.myx.ae1.messaging.MessageBlank;
import ru.myx.ae1.messaging.MessagingManager;
import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Context;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.binary.Transfer;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.xml.Xml;

/** @author myx
 *
 *         Window - Preferences - Java - Code Style - Code Templates */
final class MessagingManagerImpl implements MessagingManager {
	
	private static final BaseObject dataMaterialize(final int type, final TransferCopier data) throws Exception {
		
		switch (type) {
			case 0 :
				return BaseObject.UNDEFINED;
			case 1 :
				return Xml.toBase("messageMaterialize", data, StandardCharsets.UTF_8, null, null, null);
			case 2 :
				return Xml.toBase("messageMaterialize", data, StandardCharsets.UTF_8, null, null, null);
			default :
				throw new RuntimeException("Unknown data type: " + type);
		}
	}

	private static final String join(final int[] keys) {
		
		if (keys == null || keys.length == 0) {
			return "";
		}
		final StringBuilder result = new StringBuilder().append(keys[0]);
		for (int i = keys.length - 1; i > 0; --i) {
			result.append(',').append(keys[i]);
		}
		return result.toString();
	}

	private final Plugin parent;

	MessagingManagerImpl(final Plugin parent) {
		
		this.parent = parent;
	}

	@Override
	public final MessageBlank createBlankMessage(final String messageFactoryId, final BaseObject messageFactoryParameters) {
		
		return new MessageBlankImpl(this, messageFactoryId, messageFactoryParameters);
	}

	@Override
	public final MessageBlank createBlankMessageText(final String subject, final String body) {
		
		return new MessageBlankText(this, subject, body);
	}

	@Override
	public final void deleteInbox(final int key) {
		
		try (final Connection conn = this.parent.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is unavailable now!");
			}
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM m1Inbox WHERE msgLuid=?")) {
				ps.setInt(1, key);
				ps.execute();
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void deleteInbox(final int[] keys) {
		
		try (final Connection conn = this.parent.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is unavailable now!");
			}
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM m1Inbox WHERE msgLuid IN (" + MessagingManagerImpl.join(keys) + ")")) {
				ps.execute();
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void deleteSent(final int key) {
		
		try (final Connection conn = this.parent.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is unavailable now!");
			}
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM m1Sent WHERE msgLuid=?")) {
				ps.setInt(1, key);
				ps.execute();
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void deleteSent(final int[] keys) {
		
		try (final Connection conn = this.parent.nextConnection()) {
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM m1Sent WHERE msgLuid IN (" + MessagingManagerImpl.join(keys) + ")")) {
				ps.execute();
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final Message[] getInbox() {
		
		try (final Connection conn = this.parent.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is unavailable now!");
			}
			final PreparedStatement ps = conn.prepareStatement(
					"SELECT i.msgLuid, i.msgId, m.msgDate, i.msgRead, i.msgTarget, m.msgOwnerId, m.fcId FROM m1Inbox i, m1Messages m WHERE i.msgId=m.msgId AND i.msgUserId=? ORDER BY m.msgDate DESC",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			try {
				ps.setString(1, Context.getUserId(Exec.currentProcess()));
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						final List<Message> result = new ArrayList<>();
						do {
							result.add(
									new MessageImpl(
											this,
											rs.getInt(1),
											rs.getString(2),
											rs.getTimestamp(3).getTime(),
											true,
											"Y".equals(rs.getString(4)),
											rs.getString(5),
											rs.getString(6),
											rs.getString(7)));
						} while (rs.next());
						return result.toArray(new Message[result.size()]);
					}
					return null;
				}
			} finally {
				try {
					ps.close();
				} catch (final Throwable t) {
					// ignore
				}
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final Message getInbox(final int key) {
		
		try (final Connection conn = this.parent.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is unavailable now!");
			}
			try (final PreparedStatement ps = conn.prepareStatement(
					"SELECT i.msgLuid,i.msgId,m.msgDate,i.msgRead,i.msgTarget,m.msgOwnerId,m.fcId FROM m1Inbox i, m1Messages m WHERE i.msgId=m.msgId AND i.msgLuid=? AND i.msgUserId=?",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY)) {
				ps.setInt(1, key);
				ps.setString(2, Context.getUserId(Exec.currentProcess()));
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						return new MessageImpl(
								this,
								rs.getInt(1),
								rs.getString(2),
								rs.getTimestamp(3).getTime(),
								true,
								"Y".equals(rs.getString(4)),
								rs.getString(5),
								rs.getString(6),
								rs.getString(7));
					}
					return null;
				}
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final Message[] getSent() {
		
		try (final Connection conn = this.parent.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is unavailable now!");
			}
			try (final PreparedStatement ps = conn.prepareStatement(
					"SELECT s.msgLuid,s.msgId,m.msgDate,s.msgTarget,m.msgOwnerId,m.fcId FROM m1Sent s, m1Messages m WHERE s.msgId=m.msgId AND s.msgUserId=? ORDER BY m.msgDate DESC",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY)) {
				ps.setString(1, Context.getUserId(Exec.currentProcess()));
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						final List<Message> result = new ArrayList<>();
						do {
							result.add(
									new MessageImpl(
											this,
											rs.getInt(1),
											rs.getString(2),
											rs.getTimestamp(3).getTime(),
											true,
											true,
											rs.getString(4),
											rs.getString(5),
											rs.getString(6)));
						} while (rs.next());
						return result.toArray(new Message[result.size()]);
					}
					return null;
				}
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final Message getSent(final int key) {
		
		try (final Connection conn = this.parent.nextConnection()) {
			final PreparedStatement ps = conn.prepareStatement(
					"SELECT s.msgLuid,s.msgId,m.msgDate,s.msgTarget,m.msgOwnerId,m.fcId FROM m1Sent s, m1Messages m WHERE s.msgId=m.msgId AND s.msgLuid=? AND s.msgUserId=? AND s.msgProcessed=?",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			try {
				ps.setInt(1, key);
				ps.setString(2, Context.getUserId(Exec.currentProcess()));
				ps.setString(3, "Y");
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						return new MessageImpl(this, rs.getInt(1), rs.getString(2), rs.getTimestamp(3).getTime(), true, true, rs.getString(4), rs.getString(5), rs.getString(6));
					}
					return null;
				}
			} finally {
				try {
					ps.close();
				} catch (final Throwable t) {
					// ignore
				}
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final boolean hasInbox() {
		
		return true;
	}

	@Override
	public final boolean hasSent() {
		
		return true;
	}

	@Override
	public final String toString() {
		
		return "MManager: parent=" + this.parent;
	}

	final void commitMessageBlank(final String factoryId, final BaseObject factoryParameters, final List<String> recipients, final boolean interactive) throws Exception {
		
		try (final Connection conn = this.parent.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is unavailable now!");
			}
			conn.setAutoCommit(false);
			try {
				final String userId = Context.getUserId(Exec.currentProcess());
				final String messageId = Engine.createGuid();
				final Timestamp messageDate = new Timestamp(Engine.fastTime());
				{
					try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO m1Messages(msgId,msgOwnerId,msgDate,fcId,fcDataType,fcData) VALUES (?,?,?,?,?,?)")) {
						ps.setString(1, messageId);
						ps.setString(2, userId);
						ps.setTimestamp(3, messageDate);
						ps.setString(4, factoryId);
						ps.setInt(5, 1);
						ps.setBytes(6, Xml.toXmlString("data", factoryParameters, true).getBytes(StandardCharsets.UTF_8));
						ps.execute();
					}
				}
				{
					try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO m1Sent(msgId,msgUserId,msgTarget,msgProcessed) VALUES (?,?,?,?)")) {
						for (final String recipient : recipients) {
							ps.setString(1, messageId);
							ps.setString(2, userId);
							ps.setString(3, recipient);
							ps.setString(4, "Y");
							ps.execute();
							ps.clearParameters();
						}
					}
				}
				{
					try (final PreparedStatement ps = conn
							.prepareStatement("INSERT INTO m1Queue(msgId,msgQueued,msgExpire,msgFailCounter,msgPriority,msgInteractive,msgTarget) VALUES (?,?,?,?,?,?,?)")) {
						final String messageInteractive = interactive
							? "Y"
							: "N";
						final Timestamp messageExpire = new Timestamp(Engine.fastTime() + 60_000L * 60L * 24L * 7L);
						for (final String recipient : recipients) {
							ps.setString(1, messageId);
							ps.setTimestamp(2, messageDate);
							ps.setTimestamp(3, messageExpire);
							ps.setInt(4, 10);
							ps.setInt(5, 0);
							ps.setString(6, messageInteractive);
							ps.setString(7, recipient);
							ps.execute();
							ps.clearParameters();
						}
					}
				}
				conn.commit();
			} catch (final Error e) {
				try {
					conn.rollback();
				} catch (final Throwable t) {
					// ignore
				}
				throw e;
			} catch (final Exception e) {
				try {
					conn.rollback();
				} catch (final Throwable t) {
					// ignore
				}
				throw e;
			} catch (final Throwable e) {
				try {
					conn.rollback();
				} catch (final Throwable t) {
					// ignore
				}
				throw new RuntimeException(e);
			}
		}
	}

	final void delete(final String msgId) {
		
		try (final Connection conn = this.parent.nextConnection()) {
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM m1Queue WHERE msgId=?")) {
				ps.setString(1, msgId);
				ps.execute();
			}
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM m1Inbox WHERE msgId=?")) {
				ps.setString(1, msgId);
				ps.execute();
			}
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM m1Sent WHERE msgId=?")) {
				ps.setString(1, msgId);
				ps.execute();
			}
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM m1Messages WHERE msgId=?")) {
				ps.setString(1, msgId);
				ps.execute();
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	final BaseObject getMessageParameters(final String msgId) {
		
		try (final Connection conn = this.parent.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is unavailable now!");
			}
			try (final PreparedStatement ps = conn
					.prepareStatement("SELECT fcDataType,fcData FROM m1Messages WHERE msgId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
				ps.setString(1, msgId);
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						try {
							final int dataType = rs.getInt(1);
							if (dataType == 0) {
								return null;
							}
							final TransferCopier intData = Transfer.createBuffer(rs.getBinaryStream(2)).toBinary();
							return MessagingManagerImpl.dataMaterialize(dataType, intData);
						} catch (final Exception e) {
							throw new RuntimeException("While gathering message data", e);
						}
					}
					return null;
				}
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	final void inboxRead(final int msgLuid, final String msgId) {
		
		try (final Connection conn = this.parent.nextConnection()) {
			try (final PreparedStatement ps = conn.prepareStatement("UPDATE m1Inbox SET msgRead=? WHERE msgLuid=? AND msgId=?")) {
				ps.setString(1, "Y");
				ps.setInt(2, msgLuid);
				ps.setString(3, msgId);
				ps.execute();
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
