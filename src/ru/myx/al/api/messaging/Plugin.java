/*
 * Created on 09.10.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.al.api.messaging;

import java.sql.Connection;
import java.util.Enumeration;

import ru.myx.ae1.AbstractPluginInstance;
import ru.myx.ae3.Engine;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.help.Convert;
import ru.myx.jdbc.lock.Interest;
import ru.myx.jdbc.lock.Lock;
import ru.myx.jdbc.lock.LockManager;
import ru.myx.jdbc.lock.Runner;

/**
 * @author myx
 * 		
 *         Window - Preferences - Java - Code Style - Code Templates
 */
public class Plugin extends AbstractPluginInstance {
	
	private String identity;
	
	private String connection;
	
	private Enumeration<Connection> connectionSource;
	
	private MessagingManagerImpl manager = null;
	
	private boolean client;
	
	private Runner runner;
	
	private LockManager lockManager;
	
	@Override
	public void destroy() {
		
		if (this.lockManager != null) {
			this.lockManager.stop(this.identity);
			this.lockManager = null;
		}
		if (this.runner != null) {
			this.runner.stop();
			this.runner = null;
		}
	}
	
	MessagingManagerImpl getManager() {
		
		return this.manager;
	}
	
	Connection nextConnection() {
		
		return this.connectionSource.nextElement();
	}
	
	@Override
	public void register() {
		
		this.connectionSource = this.getServer().getConnections().get(this.connection);
		
		try {
			final BaseObject settings = this.getSettingsPrivate();
			String identity = Base.getString(settings, "identity", "").trim();
			if (identity.length() == 0) {
				identity = Engine.createGuid();
				settings.baseDefine("identity", identity);
			}
			this.identity = identity;
		} catch (final Throwable t) {
			throw new RuntimeException(t);
		}
		
		this.commitPrivateSettings();
		
		this.manager = new MessagingManagerImpl(this);
		this.getServer().setMessagingManager(this.manager);
	}
	
	@Override
	public void setup() {
		
		final BaseObject info = this.getSettingsProtected();
		this.connection = Base.getString(info, "connection", "default");
		this.client = Convert.MapEntry.toBoolean(info, "client", false);
	}
	
	@Override
	public void start() {
		
		this.lockManager = Lock.createManager(this.connectionSource, "m1Locks", this.identity);
		this.lockManager.start(this.identity);
		if (this.client) {
			return;
		}
		if (this.runner == null) {
			this.runner = new MessagingRunner(this);
		}
		if (this.lockManager == null) {
			this.runner.start();
		} else {
			this.lockManager.addInterest(new Interest("messaging", this.runner));
		}
	}
}
