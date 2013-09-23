module Hadoop::Bcftools
  # Errors defines all errors of Hadoop:Bcftools
  # @author Wei-Ming wu
  module Errors
    # HadoopNotFoundError is an Exception.
    class HadoopNotFoundError < Exception ; end
    # BcftoolsNotFoundError is an Exception.
    class BcftoolsNotFoundError < Exception ; end
    # FSDefaultNameNotFoundError is an Exception.
    class FSDefaultNameNotFoundError < Exception ; end
    # HadoopCommandNotFoundError is an Exception.
    class HadoopCommandNotFoundError < Exception ; end
    # StreamingJarNotFoundError is an Exception.
    class StreamingJarNotFoundError < Exception ; end
    # InvalidCommandError is an Exception.
    class InvalidCommandError < Exception ; end
    # RequiredFilesMissingError is an Exception.
    class RequiredFilesMissingError < Exception ; end
    # NotSupportedError is an Exception.
    class HdfsFileUploadingError < Exception ; end
    # NotSupportedError is an Exception.
    class NotSupportedError < Exception ; end
  end
end
