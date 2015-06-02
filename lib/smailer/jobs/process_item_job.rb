module Smailer
  module Jobs
    class ProcessItemJob < ActiveJob::Base
      queue_as :default

      cached_attachments = {}
      finished_mails_counts = Hash.new(0)

      Mail.defaults do
        method = Rails.configuration.action_mailer.delivery_method
        delivery_method method, Rails.configuration.action_mailer.send("#{method}_settings") || {}
      end

      def perform(max_retries, max_lifetime, bounce_prefix, id, options = {})
        # try to lock the queue item, if we were able to lock it, we go with the rest of the stuff
        if Smailer::Compatibility.update_all(Smailer::Models::QueuedMail, {:locked => true, :locked_at => Time.now.utc}, {:id => id, :locked => false}) == 1
          # we will process this now

          queue_item = Smailer::Models::QueuedMail.find(id)
          mail = Mail.new do
            from    queue_item.from
            to      queue_item.to
            subject queue_item.subject
            queue_item.mail_campaign.attachments.each do |attachment|
              cached_attachments[attachment.id] ||= attachment.body
              add_file :filename => attachment.filename,
                       :content => cached_attachments[attachment.id]
            end

            text_part { body queue_item.body_text }
            html_part { body queue_item.body_html; content_type 'text/html; charset=UTF-8' }
          end
          mail.raise_delivery_errors = true

          # compute the VERP'd return_path if requested
          # or fall-back to a global return-path if not
          item_return_path = if options[:verp]
            "#{bounce_prefix}#{queue_item.key}@#{options[:return_path_domain]}"
          else
            options[:return_path]
          end

          # set the return-path, if any
          if item_return_path
            mail.return_path   = item_return_path
            mail['Errors-To']  = item_return_path
            mail['Bounces-To'] = item_return_path
          end

          queue_item.last_retry_at = Time.now
          queue_item.retries      += 1
          queue_item.locked        = false # unlock this email

          begin
            # commense delivery
            mail.deliver
          rescue Exception => e
            # failed, we have.
            queue_item.last_error = "#{e.class.name}: #{e.message}"
            queue_item.save

            # check if the message hasn't expired;
            retries_exceeded = max_retries  > 0 && queue_item.retries >= max_retries
            too_old = max_lifetime > 0 && (Time.now - queue_item.created_at) >= max_lifetime

            if retries_exceeded || too_old
              # the message has expired; move to finished_mails
              Smailer::Models::FinishedMail.add(queue_item, Smailer::Models::FinishedMail::Statuses::FAILED, false)
              Smailer::Models::MailCampaign.update_counters queue_item.mail_campaign.id, :sent_mails_count => 1
            end
            # results.push [queue_item, :failed]
          else
            # great job, message sent
            Smailer::Models::FinishedMail.add(queue_item, Smailer::Models::FinishedMail::Statuses::SENT, false)
            Smailer::Models::MailCampaign.update_counters queue_item.mail_campaign.id, :sent_mails_count => 1
          end
        end
      end
    end
  end
end