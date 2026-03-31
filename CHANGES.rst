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
- the `rename` filter now allows applying regexp/group expansion to the name
- added the `count-data` filter for counting data items
- added `BytesSupporter` mixin
- added the `shell-exec` reader for executing arbitrary external commands
- added the `get-metadata` filter to extract field values from the meta-data
- added the `sleep` filter for waiting specified number of seconds before forwarding data
- added `--log_execution_time` flag to `sub-process`, `tee`, `trigger`
- `parse_conversion_args` and `perform_pipeline_execution` now support default placeholders HOME/CWD/TMP
- generators `csv-file`, `dirs`, `files` and `text-file` now support placeholders in their paths


0.0.1 (2025-10-31)
------------------

- initial release

