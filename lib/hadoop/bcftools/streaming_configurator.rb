require 'hadoop/bcftools/errors'

module Hadoop::Bcftools
  # StreamingConfigurator setups necessary variables for Hadoop Streaming.
  # @author Wei-Ming Wu
  module StreamingConfigurator
    include Errors
    
    # Finds out necessary variables by given path of Hadoop home.
    #
    # @return [Array] an Array contains the locations of Hadoop command, file system default name and Hadoop Streaming jar
    def config_streaming hadoop_home
      hadoop_cmd = find_hadoop_cmd hadoop_home
      fs_default_name = find_fs_default_name hadoop_home
      streaming_jar = find_streaming_jar hadoop_home
      [hadoop_cmd, fs_default_name, streaming_jar]
    end
    
    private
    
    def find_hadoop_cmd hadoop_home
      hadoop_cmd = File.join hadoop_home, 'bin', 'hadoop'
      raise HadoopCommandNotFoundError, 'Hadoop command not found.' unless File.exist? hadoop_cmd
      hadoop_cmd
    end 
    
    def find_fs_default_name hadoop_home
      core_site_path = File.join hadoop_home, 'conf', 'core-site.xml'
      raise FSDefaultNameNotFoundError, 'core-site.xml not found.' unless File.exist? core_site_path
      f = File.open core_site_path
      core_site = f.read
      f.close
      fs_default_name = core_site.match(/<name>\s*fs\.default\.name\s*<\/name>\s*<value>\s*([^<]+)\s*<\/value>/)[1]
      raise FSDefaultNameNotFoundError, 'fs.default.name not found.' unless fs_default_name
      fs_default_name
    end
    
    def find_streaming_jar hadoop_home
      streaming_jar_folder = File.join hadoop_home, 'contrib', 'streaming'
      streaming_jar = Dir[File.join streaming_jar_folder, '*'].select { |f| f =~ /hadoop-streaming-\d+\.\d+\.\d+\.jar$/ }.first
      raise StreamingJarNotFoundError, 'Streaming jar not found.' unless streaming_jar
      streaming_jar
    end
  end
end
