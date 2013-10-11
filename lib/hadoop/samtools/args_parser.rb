require 'active_support/core_ext/hash'
require 'hadoop/samtools/errors'

module Hadoop::Samtools
  # ArgsParser is used to parse Samtools command arguments.
  # @author Wei-Ming Wu
  module ArgsParser
    # CMD_FORMAT defines the number of required files for each samtools command.
    # how to listen merge file?
    CMD_FORMAT = { faidx: [1],   view: [1,2],   merge: [2],
                   sort: [2], rmdup: [2],  index: [1] ,
                   mpileup: [2] }.with_indifferent_access
    include Errors
    
    # Returns required files for a Samtools command after parsing arguments.
    #
    # @return [Array] an Array of required files
    def parse_args cmd
      args = cmd.strip.split(/\s+/)
      cmd = args.shift
      if cmd !~ %r{#{CMD_FORMAT.keys.map { |c| "^#{c}$" }.join '|'}}
        raise InvalidCommandError, "Invalid command: #{cmd}."
      end
      files = args.slice_before { |co| co =~ /^-/ }.to_a.last.delete_if { |co| co =~ /^-/ }
      files.shift while CMD_FORMAT[cmd].max < files.size
      files.keep_if { |file| file =~ /^\w+(\.\w+)+$/ } if cmd == 'view'
      unless CMD_FORMAT[cmd].include? files.size
        raise RequiredFilesMissingError,
          "Required #{CMD_FORMAT[cmd].join ' or '} file(s), " <<
          "#{CMD_FORMAT[cmd][0] - files.size} missing."
      end
      files
    end
  end
end
