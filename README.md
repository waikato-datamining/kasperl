# kasperl
<img align="right" src="img/kasperl_logo.png" width="15%" alt="kasperl logo showing a pixelated Kasperl hand puppet, based on https://en.wikipedia.org/wiki/Kasperle#/media/File:Hand_Puppet.jpg"/>

[seppl](https://github.com/waikato-datamining/seppl)-based Python library with
generic plugins for pipelines and building blocks for creating command-line
tools for pipelines.


## Installation

Via PyPI:

```bash
pip install kasperl
```

The latest code straight from the repository:

```bash
pip install git+https://github.com/waikato-datamining/kasperl.git
```


## Plugins/functions

Plugins listed as `(abstract)` typically need one or more methods implemented
that return plugins of a specific type. Ones listed as `(superclass)` are 
basic ancestors for class hierarchies, implementing mixins and other 
functionality. `(mixin)` classes can be added to relevant classes.
`(function)` denotes useful functions. 

### Mixins

* kasperl.api.NameSupporter - for record classes that manage a name
* kasperl.api.SourceSupporter - for record classes that manage a source path
* kasperl.api.AnnotationHandler - for record classes that manage annotations

### Generators

* kasperl.api.Generator (superclass)
* kasperl.api.SingleVariableGenerator
* kasperl.generator.CSVFileGenerator
* kasperl.generator.DirectoryGenerator
* kasperl.generator.ListGenerator
* kasperl.generator.NullGenerator
* kasperl.generator.PromptGenerator
* kasperl.generator.RangeGenerator
* kasperl.generator.TextFileGenerator

### Readers

* kasperl.api.Reader (superclass)
* kasperl.api.parse_reader (function)
* kasperl.api.AnnotationsOnlyReader (mixin)
* kasperl.api.add_annotations_only_reader_param (function)
* kasperl.filter.ListFiles
* kasperl.filter.PollDir (abstract)
* kasperl.filter.Start
* kasperl.filter.Storage
* kasperl.filter.TextFile

### Filters

* kasperl.api.parse_filter (function)
* kasperl.filter.Block
* kasperl.filter.CheckDuplicateFilenames
* kasperl.filter.DiscardByName
* kasperl.filter.DiscardNegatives
* kasperl.filter.ListToSequence
* kasperl.filter.MaxRecords
* kasperl.filter.MetaData
* kasperl.filter.MetaDataFromName
* kasperl.filter.MetaDataToPlaceholder
* kasperl.filter.MoveFiles
* kasperl.filter.PassThrough
* kasperl.filter.RandomizeRecords
* kasperl.filter.RecordWindow
* kasperl.filter.Rename
* kasperl.filter.Sample
* kasperl.filter.SetMetaData
* kasperl.filter.SetPlaceholder
* kasperl.filter.SplitRecords
* kasperl.filter.Stop
* kasperl.filter.Storage
* kasperl.filter.SubProcess (abstract)
* kasperl.filter.Tee (abstract)
* kasperl.filter.Trigger (abstract)

### Writers

* kasperl.api.parse_writer (function)
* kasperl.api.BatchWriter (superclass)
* kasperl.api.SplittableBatchWriter (superclass)
* kasperl.api.StreamWriter (superclass)
* kasperl.api.SplittableStreamWriter (superclass)
* kasperl.api.AnnotationsOnlyWriter (mixin)
* kasperl.api.add_annotations_only_param (function)
* kasperl.writer.Console
* kasperl.writer.SendEmail (abstract)
* kasperl.writer.Storage
* kasperl.writer.TextFile

### Other

* kasperl.api.Session - session object that gets shared by the plugins, extended seppl class
* kasperl.api.strip_suffix (function)
* kasperl.api.locate_file (function)
* kasperl.api.load_function (function)
* kasperl.api.safe_deepcopy (function)
* kasperl.api.compare_values, COMPARISONS, ... - function/constants for conditional processing


## Tools

The `perform_XYZ` functions are usually the only ones needed for the 
command-line tools. Other methods listed provide control over specific 
parts of the tool execution and are typically used by the `perform_XYZ`
functions.

### Conversion

* kasperl.api.perform_conversion
* kasperl.api.parse_conversion_args
* kasperl.api.print_conversion_usage

### Generators

* kasperl.api.perform_generator_test
* kasperl.api.test_generator

### Pipeline execution

* kasperl.api.perform_pipeline_execution
* kasperl.api.execute_pipeline

### Finding files

* kasperl.api.perform_find_files
* kasperl.api.find_files_parser 
* kasperl.api.find_files
