# Changelog

All notable changes to the [Smithsonian/smax-python](https://github.com/Smithsonian/smax-clib) library will be 
documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to 
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added

 - Automatic dependabot updates. (by @attipaci)
 - GitHub CodeQL workflow. (by @attipaci)
 - Added license, contributors' guide, code of conduct, changelog.
 
### Changed

 - #15: GitHub pages deployed from dynamically built documentation using GitHub Actions and the current source. (by 
   @attipaci)
 - #14: Bumped GitHub Actions workflow actions versions to latest. (by @attipaci)
 - README edits. (by @attipaci)

### Removed

 - #13: `redis_config_files` submodule. These were needed for testing only, but there are better better ways to do that
   without using submodules. (by @attipaci)
 - Removed old static API documentation. (by @attipaci)
 

## [1.0.3] - 2024-08-30
 
### Changed

 - Remove extraneous print statements that were producing strange output. (by @PaulKGrimes)


## [1.0.2] - 2024-08-14

### Changed

 - Alter the smax command line interface to work with one or both of the `-t` and `-k` options. (by @PaulKGrimes)

## [1.0.1] - 2024-08-13

### Added

 - Network failover and data classes testing. (by @PaulKGrimes)
 - #11: Utilities. (by @PaulKGrimes)
 

## [1.0.0] - 2023-05-10

Initial release.
