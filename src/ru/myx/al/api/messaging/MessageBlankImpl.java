/*
 * Created on 09.10.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.al.api.messaging;

import java.util.ArrayList;
import java.util.List;

import ru.myx.ae1.messaging.MessageBlank;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class MessageBlankImpl implements MessageBlank {
	private final List<String>			recipients	= new ArrayList<>();
	
	private final MessagingManagerImpl	manager;
	
	private final String				factoryId;
	
	private final BaseObject			factoryParameters;
	
	private boolean						interactive	= false;
	
	MessageBlankImpl(final MessagingManagerImpl manager, final String factoryId, final BaseObject factoryParameters) {
		this.manager = manager;
		this.factoryId = factoryId;
		this.factoryParameters = factoryParameters == null
				? new BaseNativeObject()
				: factoryParameters;
	}
	
	@Override
	public final void addRecipientAccess(final String path, final String permission) {
		this.recipients.add( "#access: " + permission + ", " + path );
	}
	
	@Override
	public final void addRecipientEmail(final String email) {
		this.recipients.add( "#email: " + email );
	}
	
	@Override
	public final void addRecipientGroupId(final String groupId) {
		this.recipients.add( "#group: " + groupId );
	}
	
	@Override
	public final void addRecipientUserId(final String userId) {
		this.recipients.add( "#uid: " + userId );
	}
	
	@Override
	public final void commit() {
		if (this.recipients.isEmpty()) {
			throw new IllegalArgumentException( "No recipients in message!" );
		}
		try {
			this.manager.commitMessageBlank( this.factoryId, this.factoryParameters, this.recipients, this.interactive );
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException( e );
		}
	}
	
	@Override
	public final void setSendInteractive(final boolean interactive) {
		this.interactive = interactive;
	}
}
