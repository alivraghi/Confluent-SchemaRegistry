use Config;
use inc::Module::Install;

license 'MIT';
version_from 'lib/Confluent/SchemaRegistry.pm';
readme_from 'lib/Confluent/SchemaRegistry.pm';
all_from 'lib/Confluent/SchemaRegistry.pm';
build_requires 'Test::More', 0.88;
test_requires 'Math::BigInt';
test_requires 'Test::Exception';
requires 'JSON::XS';
requires 'REST::Client';
requires 'Try::Tiny';
requires 'Avro::Schema';
requires 'Aspect';

WriteMakefile(
    NAME => 'Confluent::SchemaRegistry',
    META_MERGE => {
        resources => {
            repository  => 'https://github.com/alivraghi/Confluent-SchemaRegistry'
        }
    }
);
