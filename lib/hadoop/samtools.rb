require 'hadoop/samtools/version'
require 'hadoop/samtools/runner'

# Module Hadoop defines a namespace for Hadoop Ruby Utils
module Hadoop
  # Hadoop::Bwa includes tools to run BWA on Hadoop Streaming.
  # @author Wei-Ming Wu
  module Samtools
    # Creates a Hadoop::Samtools::Runner.
    #
    # @return [Runner] a Runner object
    def self.new opts = {}
      Runner.new opts
    end
  end
  
  module Bcftools
    # Creates a Hadoop::Bcftools::Runner.
    #
    # @return [Runner] a Runner object
    def self.new opts = {}
      Runner.new opts
    end
  end
end