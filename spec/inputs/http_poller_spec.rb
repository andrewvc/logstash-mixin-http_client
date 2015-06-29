require "logstash/devutils/rspec/spec_helper"
require 'logstash/inputs/http_poller'

describe LogStash::Inputs::HTTP_Poller do
  let(:queue) { Queue.new }
  let(:default_interval) { 5 }
  let(:default_name) { "url1 " }
  let(:default_url) { "http://localhost:1827" }
  let(:default_urls) {
    {
      default_name => default_url
    }
  }
  let(:default_opts) {
    {
      "interval" => default_interval,
      "urls" => default_urls,
      "codec" => "json"
    }
  }
  let(:mod) { LogStash::Inputs::HTTP_Poller }

  subject { mod.new(default_opts) }

  describe "#run" do
    it "should run at the specified interval" do
      expect(Stud).to receive(:interval).with(default_interval).once
      subject.send(:run, double("queue"))
    end
  end

  describe "#run_once" do
    it "should issue an async request for each url" do
      default_urls.each { |name, url|
        expect(subject).to receive(:request_async).with(name, url).once
      }

      subject.send(:run_once, double("queue"))
    end
  end

  shared_examples("matching metadata") {
    let(:metadata) { event["@metadata"]["http_poller"] }

    it "should have the correct name" do
      metadata["name"].should eql(name)
    end

    it "should have the correct url" do
      metadata["url"].should eql(url)
    end

    it "should have the correct code" do
      metadata["code"].should eql(code)
    end
  }

  shared_examples "unprocessable_requests" do
    let(:logger) { poller.instance_variable_get(:@logger) }
    let(:poller) { LogStash::Inputs::HTTP_Poller.new(settings) }
    subject {
      poller.send(:run_once, queue)
      queue.pop(true)
    }

    before do
      subject # materialize the subject
    end

    it "should enqueue a message" do
      expect(subject).to be_a(LogStash::Event)
    end

    it "should enqueue a message with '_http_request_failure' set" do
      expect(subject["_http_request_failure"]).to be_a(Hash)
    end

    include_examples("matching metadata")
  end

  describe "with a non responsive server" do
    describe "due to a non-existant host" do
      let(:name) { default_name }
      let(:url) { "thouetnhoeu89ueoueohtueohtneuohn" }
      let(:code) { nil } # no response expected

      let(:settings) { default_opts.merge("urls" => { name => url}) }

      include_examples("unprocessable_requests")
    end

    describe "due to a bogus port number" do
      let(:settings) { default_opts.merge("urls" => {"one" => "http://127.0.0.1:9999999"}) }

      include_examples("unprocessable_requests")
    end
  end

  describe "a codec mismatch" do
    let(:inst) {
      mod.new(default_opts)
    }

    let(:qmsg) {
      inst.client.stub(default_url, body: "Definitely not JSON!", code: 200)
      inst.send(:run_once, queue)
      queue.pop(true)
    }

    it "should send a _jsonparsefailure" do
      expect(qmsg["tags"]).to include("_jsonparsefailure")
    end
  end

  describe "a valid decoded response" do
    let(:payload) {
      {"a" => 2, "hello" => ["a", "b", "c"]}
    }
    let(:inst) {
      mod.new(default_opts)
    }

    let(:qmsg) {
      inst.client.stub(default_url, body: LogStash::Json.dump(payload), code: 202)
      inst.send(:run_once, queue)
      queue.pop(true)
    }

    it "should have a matching message" do
      expect(qmsg.to_hash).to include(payload)
    end
  end
end
