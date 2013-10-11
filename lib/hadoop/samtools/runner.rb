require 'active_support/core_ext/hash'
require 'hadoop/samtools/streaming_configurator'
require 'hadoop/samtools/args_parser'
require 'hadoop/samtools/hdfs_uploader'
require 'hadoop/samtools/errors'

module Hadoop::Samtools
  # Runner is used to run Samtools commands on Hadoop Streaming.
  # @author Wei-Ming Wu
  class Runner
    # SAMTOOLS_PREREQUISITE defines necessary files for Samtools commands.
    # Samtools -VERSION 0.1.18 
    SAMTOOLS_PREREQUISITE = { faidx: [],  view: [], 
                         sort: [],rmdup: [], index: [],
                         mpileup: ['bai','fai'] }.with_indifferent_access
    include StreamingConfigurator
    include ArgsParser
    include Errors
    attr_reader :hadoop_home, :samtools, :hadoop_cmd, :fs_default_name, :streaming_jar
    
    # Creates a Runner.
    #
    # @param [Hash] opts the options of this Runner
    # @option [String] :hadoop_home the location of Hadoop home
    # @option [String] :samtools the location of Samtools command
    # @option [String] :bcftools the location of Bcftools command
    # @return [Runner] a Runner object
    def initialize opts = {}
      opts = opts.with_indifferent_access
      @hadoop_home = opts[:hadoop_home] || ENV['HADOOP_HOME'] ||
        raise(HadoopNotFoundError,
          'Hadoop home not found. ' <<
          'Please set system variable HADOOP_HOME or ' <<
          'passing :hadoop_home => path_to_hadoop_home.')
      @samtools = opts[:samtools] || which('samtools') ||
        raise(SamtoolsNotFoundError,
          'Samtools not found. ' <<
          'Please install Samtools first or passing :samtools => path_to_samtools.')  
        
      @hadoop_cmd, @fs_default_name, @streaming_jar = config_streaming @hadoop_home
      @uploader = HdfsUploader.new @hadoop_cmd
    end
    
    # Runs a Samtools command.
    #
    # @param [String] cmd a Samtools command
    # @param [Hash] opts the options of method `run`
    # @option [String] :local the folder where local files located
    # @option [String] :hdfs the folder where HDFS files should be found
    def run cmd, opts = {}
      local = opts[:local] || '.'
      hdfs = opts[:hdfs] || "/user/#{ENV['USER']}"
      system "#{@hadoop_cmd} fs -touchz #{File.join hdfs, 'hadoop-samtools-streaming-input.txt'}"
      files = parse_args cmd
      @uploader.upload_files local, hdfs, files
      streaming cmd, local, hdfs, files
    end
    
    # Creates a Hadoop Streaming statement.
    #
    # @param [String] cmd a Samtools command
    # @param [String] hdfs the folder where HDFS files should be found
    # @param [Array] files an Array contains all names of required files
    def streaming_statement cmd, hdfs, files
      puts 'Streaming statement:'
      stmt = "#{@hadoop_cmd} jar #{@streaming_jar} " <<
      "-files #{files.map { |f| "#{File.join @fs_default_name, hdfs, f}" }.join ','} " <<
      "-input #{File.join @fs_default_name, hdfs, 'hadoop-samtools-streaming-input.txt'} " <<
      "-output \"#{File.join hdfs, 'hadoop-samtools-' + cmd.split(/\s+/)[0] + '_' + Time.now.to_s.split(/\s+/).first(2).join.chars.keep_if { |c| c=~ /\d/ }.join}\" " <<
      "-mapper \"#{@samtools} #{cmd}\" " <<
      "-reducer NONE"
      puts stmt
      stmt
    end
    
    private
    
    def streaming cmd, local, hdfs, files
      samtools_cmd = cmd.split(/\s+/)[0]
      puts "Execuating Samtools #{samtools_cmd}, required files: #{files}"
      puts "Preparing for Hadoop Streaming..."
      case samtools_cmd
      when 'fadix'
        `#{@samtools} #{cmd}`
        exts = %w(fai)
        @uploader.upload_files local, hdfs, exts.map { |ext| "#{files[0]}.#{ext}" }
      when 'index'
        `#{@samtools} #{cmd}`
        exts = %w(bai)
        @uploader.upload_files local, hdfs, exts.map { |ext| "#{files[0]}.#{ext}" }
      when 'merge'
        raise NotSupportedError, 'MergeNotSupport.'
      when 'rmdup'
        raise NotSupportedError, 'RmdupNotSupport.'    
      when 'view'
        files = files + SAMTOOLS_PREREQUISITE['view'].map { |ext| "#{files[0]}.#{ext}" }
        @uploader.upload_files local, hdfs, files
        system "#{streaming_statement cmd, hdfs, files}"
      when 'sort'
        files = files + SAMTOOLS_PREREQUISITE['sort'].map { |ext| "#{files[0]}.#{ext}" }
        @uploader.upload_files local, hdfs, files
        system "#{streaming_statement cmd, hdfs, files}"
      when 'mpileup'
        files = files + SAMTOOLS_PREREQUISITE['mpileup'].map { |ext| "#{files[0]}.#{ext}" }
        @uploader.upload_files local, hdfs, files
        system "#{streaming_statement cmd, hdfs, files}"
      else
        raise InvalidCommandError, "Invalid command: #{cmd.split(/\s+/)[0]}."
      end
    end
    
    def which cmd
      exts = ENV['PATHEXT'] ? ENV['PATHEXT'].split(';') : ['']
      ENV['PATH'].split(File::PATH_SEPARATOR).each do |path|
        exts.each do |ext|
          exe = File.join(path, "#{cmd}#{ext}")
          return exe if File.executable? exe
        end
      end
      nil
    end
  end
end
