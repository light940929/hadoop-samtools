require 'hadoop/bcftools/errors'

module Hadoop::Bcftools
  # HdfsUploader uploads files from local to HDFS.
  # @author Wei-Ming Wu
  class HdfsUploader
    include Errors
    
    # Creates a HdfsUploader.
    #
    # @param [String] hadoop_cmd the path of Hadoop command
    def initialize hadoop_cmd
      @hadoop_cmd = hadoop_cmd
    end
    
    # Uploads files from local to HDFS.
    #
    # @param [String] local where local files are located
    # @param [String] hdfs where files should be uploaded to HDFS
    # @param [Array] files specified files to be uploaded
    def upload_files local, hdfs, files
      local_files = Dir[File.join local, '*'].map { |f| File.basename f }
      remote_files = ls_remote(hdfs).map { |f| File.basename f }
      unuploaded_files = files - remote_files
      if (unuploaded_files - local_files).any?
        raise HdfsFileUploadingError,
          "Files: #{unuploaded_files - local_files} " <<
          "can't be found on either local or hdfs."
      end
      (files - (files - remote_files)).each { |f| puts "File: #{f} existed on HDFS."  }
      upload_hdfs hdfs, unuploaded_files.map { |f| File.join local, f }
    end
    
    private
    
    def upload_hdfs hdfs, files
      puts 'Uploading files to HDFS...'
      files.each do |f|
        puts "Uploading file: #{f}"
        system "#{@hadoop_cmd} fs -put #{f} #{File.join hdfs, File.basename(f)}"
      end
      puts 'Done.'
    end
    
    def ls_remote hdfs
      result = `#{@hadoop_cmd} fs -ls #{hdfs}`
      result.split("\n").drop(1).map { |line| line.split(/ /).slice_before(/^\//).to_a.last.join(' ') }
    end
  end  
end
