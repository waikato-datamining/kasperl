Changelog
=========

0.0.2 (????-??-??)
------------------

- the `locate_file` method now supports looking for files with different image/annotation prefixes
- added `croniter` dependency
- added the `cron` dummy reader which outputs a string according to the provided execution expression
  (e.g., *every 10 seconds* or *every 5 minutes on workdays*)
- `load_pipeline` removes comments now
- added the `log-data` filter for logging information about the data passing through
- the `set-metadata` filter now expands placeholders in the value if of type 'string'


0.0.1 (2025-10-31)
------------------

- initial release

