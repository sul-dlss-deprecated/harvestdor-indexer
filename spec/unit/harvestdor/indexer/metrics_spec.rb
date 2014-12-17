require 'spec_helper'

describe Harvestdor::Indexer::Metrics do
  it "should record successes" do
    expect { subject.success! }.to change { subject.success_count }.from(0).to(1)
  end

  it "should record errors" do
    expect { subject.error! }.to change { subject.error_count }.from(0).to(1)
  end

  describe "#total" do
    it "should be the sum of the successes and errors" do
      expect do 
        subject.error!
        subject.success!
      end.to change { subject.total }.from(0).to(2)
    end
  end

  describe "#tally" do
    it "should record a success if the block doesn't fail" do
      expect do
        subject.tally do
          #noop
        end
      end.to change { subject.success_count }.from(0).to(1)
    end
    
    it "should record an error if the block fails" do
      expect do
        subject.tally do
          raise "Broken"
        end
      end.to change { subject.error_count }.from(0).to(1)
    end

    it "should allow an error handler to be provided" do
      x = double
      expect(x).to receive(:call).with(kind_of(RuntimeError))
      subject.tally(on_error: x) do
        raise "Broken"
      end
    end
  end
end