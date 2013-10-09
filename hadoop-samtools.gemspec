# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'hadoop/samtools/version'

Gem::Specification.new do |spec|
  spec.name          = "hadoop-samtools"
  spec.version       = Hadoop::Samtools::VERSION
  spec.authors       = ["Wei-Ming Wu", "Han-Jen Lin"]
  spec.email         = ["wnameless@gmail.com", "linhanjen@gmail.com"]
  spec.description   = %q{SAMTOOLS for Hadoop Streaming}
  spec.summary       = %q{SAMTOOLS for Hadoop Streaming}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]


  spec.add_runtime_dependency "activesupport", ">= 3.2.0"

  spec.add_development_dependency "simplecov"
  spec.add_development_dependency "yard"
  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
end
