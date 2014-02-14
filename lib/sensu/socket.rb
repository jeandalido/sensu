module Sensu
  class Socket < EM::Connection
    attr_accessor :logger, :settings, :amq, :reply

    def post_init
      @parser = Yajl::Parser.new(:symbolize_keys => true)
      @parser.on_parse_complete = method(:object_parsed)
    end

    def respond(data)
      unless @reply == false
        send_data(data)
      end
    end

    def object_parsed(object)
      @logger.debug('socket received json data', {
        :data => object.to_s
      })
      begin
        object[:issued] = Time.now.to_i
        object[:status] ||= 0
        validates = [
          object[:name] =~ /^[\w\.-]+$/,
          object[:output].is_a?(String),
          object[:status].is_a?(Integer)
        ].all?
        if validates
          payload = {
            :client => @settings[:client][:name],
            :check => object
          }
          @logger.info('publishing check result', {
            :payload => payload
          })
          @amq.direct('results').publish(Oj.dump(payload))
          respond('ok')
        else
          @logger.warn('invalid check result', {
            :check => check
          })
          respond('invalid')
        end
      rescue Oj::ParseError => error
        @logger.warn('check result must be valid json', {
          :data => data,
          :error => error.to_s
        })
        respond('invalid')
      end
    end

    def receive_data(data)
      if data =~ /[\x80-\xff]/n
        @logger.warn('socket received non-ascii characters')
        respond('invalid')
      elsif data.strip == 'ping'
        @logger.debug('socket received ping')
        respond('pong')
      else
        @parser << data
      end
    end
  end

  class SocketHandler < EM::Connection
    attr_accessor :on_success, :on_error

    def connection_completed
      @connected_at = Time.now.to_f
      @inactivity_timeout = comm_inactivity_timeout
    end

    def unbind
      if @connected_at
        elapsed_time = Time.now.to_f - @connected_at
        if elapsed_time >= @inactivity_timeout
          @on_error.call('socket inactivity timeout')
        else
          @on_success.call('wrote to socket')
        end
      else
        @on_error.call('failed to connect to socket')
      end
    end
  end
end
