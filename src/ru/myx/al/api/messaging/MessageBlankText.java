/*
 * Created on 10.05.2006
 */
package ru.myx.al.api.messaging;

import java.util.ArrayList;
import java.util.List;

import ru.myx.ae1.BaseRT3;
import ru.myx.ae1.messaging.MessageBlank;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.email.Email;
import ru.myx.ae3.email.EmailSender;
import ru.myx.sapi.RuntimeEnvironment;

final class MessageBlankText implements MessageBlank {
	private final List<String>			recipients	= new ArrayList<>();
	
	private final MessagingManagerImpl	manager;
	
	private final String				subject;
	
	private final String				body;
	
	private boolean						interactive	= false;
	
	MessageBlankText(final MessagingManagerImpl manager, final String subject, final String body) {
		this.manager = manager;
		this.subject = subject;
		this.body = body;
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
		if (this.interactive) {
			try {
				this.manager.commitMessageBlank( "STDMESSAGE_TEXT",
						new BaseNativeObject( "body", this.body ),
						this.recipients,
						this.interactive );
			} catch (final RuntimeException e) {
				throw e;
			} catch (final Exception e) {
				throw new RuntimeException( e );
			}
		} else {
			for (final String recipient : this.recipients) {
				final Email email = new Email( "", recipient, this.subject, this.body );
				email.setSendPlain( true );
				final RuntimeEnvironment rt3 = BaseRT3.runtime();
				assert rt3 != null : "rt3 is not accessible";
				final EmailSender sender = rt3.getEmailSender();
				assert sender != null : "sender is not accessible";
				sender.sendEmail( email );
			}
		}
	}
	
	@Override
	public final void setSendInteractive(final boolean interactive) {
		this.interactive = interactive;
	}
}
