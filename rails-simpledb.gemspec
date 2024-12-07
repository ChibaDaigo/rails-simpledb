# frozen_string_literal: true

require_relative "lib/rails/simpledb/version"

Gem::Specification.new do |spec|
  spec.name = "rails-simpledb"
  spec.version = Rails::Simpledb::VERSION
  spec.authors = ["{bigfive}"]
  spec.email = ["bigfive0907@gmail.com"]

  spec.summary = "rails-simpledb create figure of simple database schema for Rails."
  spec.description = "TODO: Write a short summary, because RubyGems requires one."
  spec.homepage = "https://github.com/ChibaDaigo/rails-simpledb"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 2.6.0"

  spec.metadata["allowed_push_host"] = "https://rubygems.org"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/ChibaDaigo/rails-simpledb"
  spec.metadata["changelog_uri"] = "https://github.com/ChibaDaigo/rails-simpledb/blob/main/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").reject do |f|
      (File.expand_path(f) == __FILE__) || f.start_with?(*%w[bin/ test/ spec/ features/ .git .circleci appveyor])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # Uncomment to register a new dependency of your gem
  # spec.add_dependency "example-gem", "~> 1.0"

  # For more information and examples about making a new gem, check out our
  # guide at: https://bundler.io/guides/creating_gem.html
end
