require 'mail'

module Smailer
  module Tasks
    class Send
      def self.execute(options = {})
        options.reverse_merge! :verp => !options[:return_path_domain].blank?

        # validate options
        if options[:verp] && options[:return_path_domain].blank?
          raise "VERP is enabled, but a :return_path_domain option has not been specified or is blank."
        end

        batch_size   = (Smailer::Models::Property.get('queue.batch_size') || 100).to_i
        max_retries  = (Smailer::Models::Property.get('queue.max_retries') || 0).to_i
        max_lifetime = (Smailer::Models::Property.get('queue.max_lifetime') || 172800).to_i
        bounce_prefix = (Smailer::Models::Property.get('verp.bounce_prefix') || Smailer::BOUNCES_PREFIX)

        results = []

        # clean up any old locked items
        expired_locks_condition = ['locked = ? AND locked_at <= ?', true, 1.hour.ago.utc]
        Smailer::Compatibility.update_all(Smailer::Models::QueuedMail, {:locked => false}, expired_locks_condition)

        # load the queue items to process
        queue_sort_order = 'retries ASC, id ASC'
        items_to_process = if Smailer::Compatibility.rails_3_or_4?
          Smailer::Models::QueuedMail.select(:id).where(:locked => false).order(queue_sort_order).limit(batch_size)
        else
          Smailer::Models::QueuedMail.select(:id).all(:conditions => {:locked => false}, :order => queue_sort_order, :limit => batch_size)
        end

        # we don't need to lock anything right away. We lock anything when it's being processed.
        # # lock the queue items
        # lock_condition = {:id => items_to_process.map(&:id), :locked => false}
        # lock_update = {:locked => true, :locked_at => Time.now.utc}
        # Smailer::Compatibility.update_all(Smailer::Models::QueuedMail, lock_update, lock_condition)

        # map of attachment ID to contents - so we don't keep opening files
        # or URLs
        cached_attachments = {}
        finished_mails_counts = Hash.new(0)

        items_to_process.each do |queue_item|
          # try to send the email
          if defined? ActiveJob::Base
            # use the active job interface
            Smailer::Jobs::ProcessItemJob.perform_later(max_retries, max_lifetime, bounce_prefix, queue_item.id, options)
          end
        end

        finished_mails_counts.each do |mail_campaign_id, finished_mails_for_campaign|
          Smailer::Models::MailCampaign.update_counters mail_campaign_id, :sent_mails_count => finished_mails_for_campaign
        end

        results
      end
    end
  end
end
