# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/http_client"
require "socket" # for Socket.gethostname
require "manticore"

# Note. This plugin is a WIP! Things will change and break!
#
# Reads from a list of urls and decodes the body of the response with a codec
# The config should look like this:
#
#     input {
#       http_poller {
#         urls => {
#           "test1" => "http://localhost:9200"
#		        "test2" => "http://localhost:9200/_cluster/health"
#         }
#         request_timeout => 60
#         interval => 10
#       }
#    }

class LogStash::Inputs::HTTP_Poller < LogStash::Inputs::Base
  include LogStash::PluginMixins::HttpClient

  config_name "http_poller"

  default :codec, "json"

  # A Hash of urls in this format : "name" => "url"
  # The name and the url will be passed in the outputed event
  #
  config :urls, :validate => :hash, :required => true

  # How often  (in seconds) the urls will be called
  config :interval, :validate => :number, :required => true

  public
  def register
    @host = Socket.gethostname.force_encoding(Encoding::UTF_8)

    @logger.info("Registering http_poller Input", :type => @type,
                 :urls => @urls, :interval => @interval, :timeout => @timeout)
  end # def register

  public
  def run(queue)
    Stud.interval(@interval) do
      run_once(queue)
    end
  end

  private
  def run_once(queue)
    @urls.each do |name, url|
      request_async(queue, name, url)
    end

    # Some exceptions are only returned here, in this manner
    # For example, if the client flat out cannot execute a callback
    client.execute!
  end

  private
  def request_async(queue, name, url)
    @logger.debug? && @logger.debug("Will get url '#{name}' '#{url}")
    client.async.get(url).
      on_success {|response| handle_success(queue, name, url, response)}.
      on_failure {|exception| handle_failure(queue, name, url, exception)}.
      on_complete {|response| # on_failure does not catch them all
      if response.exception
        handle_failure(queue,name,url,exception)
      end
    }
  end

  private
  def handle_success(queue, name, url, response)
    @codec.decode(response.body) do |decoded|
      handle_decoded_event(queue, name, url, response, decoded)
    end
  end

  private
  def handle_decoded_event(queue, name, url, response, event)
    event["@metadata"] = event_metadata(name, url, response)
    queue << event
  rescue StandardError, java.lang.Exception => e
    @logger.error? && @logger.error("Error eventifying response!", exception: e, name: name, url: url, response: response)
  end

  private
  def handle_failure(queue, name, url, exception)
    event = LogStash::Event.new
    event["@metadata"] = event_metadata(name,url)
    event["_http_request_failure"] = {
      "url" => url,
      "name" => name,
      "error" => exception.to_s
    }

    queue << event
  rescue StandardError, java.lang.Exception => e
      @logger.error("Cannot read URL! Also, couldn't send the error as an event!(#{exception}/#{exception.message})", :name => name, :url => url)
  end

  private
  def event_metadata(name, url, response=nil)
    m = {
        "name" => name,
        "host" => @host,
        "url" => url
      }
    m["response_code"] = response.code if response

    {"http_poller" => m}
  end
end
