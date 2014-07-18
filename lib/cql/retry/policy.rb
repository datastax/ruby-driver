# encoding: utf-8

module Cql
  module Retry
    module Policy
      # Public: decides wether to retry a read and at what consistency level.
      # 
      # Not that this method may be calld even if required_responses >=
      # received responses if data_present is false.
      # 
      # statement          - the original Statement that timed out
      # consistency_level  - original ConsistencyLevel
      # responses_required - the number of responses required to achieve
      #                      requested consistency level
      # responses_received - the number of responses received by the time the
      #                      query timed out
      # data_received      - whether actual data (as opposed to data checksum)
      #                      was present in the received responses.
      # attempt            - the number of retries already performed
      # 
      # Returns a Cql::Policies::Retry::Decision
      def read_timeout(statement, consistency_level, responses_required,      \
                       responses_received, data_retrieved, attempt)
        raise NotImplemented, "must be implemented by a policy"
      end

      # Public: decides wether to retry a write and at what consistency level.
      # 
      # statement          - the original Statement that timed out
      # consistency_level  - original ConsistencyLevel
      # write_type         - One of :simple, :batch, :unlogged_batch, :counter
      #                      :batch_log or :cas
      # acks_required      - the number of acks required to achieve requested
      #                      consistency level
      # acks_received      - the number of acks received by the time the query
      #                      timed out
      # attempt            - the number of retries already performed
      # 
      # Returns a Cql::Policies::Retry::Decision
      def write_timeout(statement, consistency_level, write_type,             \
                               acks_required, acks_received, attempt)
        raise NotImplemented, "must be implemented by a policy"
      end

      # Public: decides wether to retry and at what consistency level on an
      #         Unavailable exception.
      # 
      # statement          - the original Statement that timed out
      # consistency_level  - original ConsistencyLevel
      # replicas_required  - the number of replicas required to achieve
      #                      requested consistency level
      # replicas_alive     - the number of replicas received by the time the
      #                      query timed out
      # attempt            - the number of retries already performed
      # 
      # Returns a Cql::Policies::Retry::Decision
      def unavailable(statement, consistency_level, replicas_required,        \
                      replicas_alive, attempt)
        raise NotImplemented, "must be implemented by a policy"
      end

      private

      def retry(consistency_level)
        Decisions::Retry.new(consistency_level)
      end

      def reraise
        DECISION_RERAISE
      end

      def ignore
        DECISION_IGNORE
      end
    end
  end
end
