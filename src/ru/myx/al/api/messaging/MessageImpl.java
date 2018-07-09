/*
 * Created on 10.10.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.al.api.messaging;

import ru.myx.ae1.messaging.Message;
import ru.myx.ae1.messaging.MessageBlank;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
class MessageImpl implements Message {
	private final MessagingManagerImpl	manager;
	
	final int							msgLuid;
	
	final String						msgId;
	
	final long							msgDate;
	
	final boolean						msgInteractive;
	
	boolean								msgRead;
	
	final String						msgTarget;
	
	final String						msgOwnerId;
	
	final String						fcId;
	
	private BaseObject				parameters	= null;
	
	MessageImpl(final MessagingManagerImpl manager,
			final int msgLuid,
			final String msgId,
			final long msgDate,
			final boolean msgInteractive,
			final boolean msgRead,
			final String msgTarget,
			final String msgSender,
			final String fcId) {
		this.manager = manager;
		this.msgLuid = msgLuid;
		this.msgId = msgId;
		this.msgDate = msgDate;
		this.msgInteractive = msgInteractive;
		this.msgRead = msgRead;
		this.msgTarget = msgTarget;
		this.msgOwnerId = msgSender;
		this.fcId = fcId;
	}
	
	@Override
	public MessageBlank createReply(final String messageFactoryId, final BaseObject messageFactoryParameters) {
		final MessageBlank blank = this.manager.createBlankMessage( messageFactoryId, messageFactoryParameters );
		blank.addRecipientUserId( this.msgOwnerId );
		return blank;
	}
	
	@Override
	public void deleteAll() {
		this.manager.delete( this.msgId );
	}
	
	@Override
	public void doneRead() {
		if (!this.msgRead) {
			this.manager.inboxRead( this.msgLuid, this.msgId );
			this.msgRead = true;
		}
	}
	
	@Override
	public long getMessageDate() {
		return this.msgDate;
	}
	
	@Override
	public String getMessageFactoryId() {
		return this.fcId;
	}
	
	@Override
	public BaseObject getMessageFactoryParams() {
		if (this.parameters == null) {
			synchronized (this) {
				if (this.parameters == null) {
					final BaseObject map = this.manager.getMessageParameters( this.msgId );
					if (map == null) {
						this.parameters = new BaseNativeObject();
					} else {
						this.parameters = map;
					}
				}
			}
		}
		return this.parameters;
	}
	
	@Override
	public String getMessageId() {
		return this.msgId;
	}
	
	@Override
	public int getMessageLuid() {
		return this.msgLuid;
	}
	
	@Override
	public boolean getMessageRead() {
		return this.msgRead;
	}
	
	@Override
	public String getMessageSender() {
		return this.msgOwnerId;
	}
	
	@Override
	public String getMessageTarget() {
		return this.msgTarget;
	}
	
	@Override
	public String toString() {
		return "msg{luid=" + this.msgLuid + ", mid=" + this.msgId + ", target=" + this.msgTarget + "}";
	}
}
