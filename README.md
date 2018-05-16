# NAME

Confluent::SchemaRegistry - A simple client for interacting with **Confluent Schema Registry**.

# SYNOPSIS

    use Confluent::SchemaRegistry;
     
    my $sr = Confluent::SchemaRegistry->new( { host => 'https://my-schema-registry.org' });
    

# DESCRIPTION

`Confluent::SchemaRegistry` provides a simple way to interact with **Confluent Schema Registry**
([https://docs.confluent.io/current/schema-registry/docs/index.html](https://docs.confluent.io/current/schema-registry/docs/index.html)) enabling writing into 
**Apache Kafka** ([https://kafka.apache.org/](https://kafka.apache.org/)) according to _Apache Avro_ schema specification 
([https://avro.apache.org/](https://avro.apache.org/)).

# METHODS

## Construction

### new( \[%config\] )

Construct a new `Confluent::SchemaRegistry`. Takes an optional hash that provides 
configuration flags for the [REST::Client](https://metacpan.org/pod/REST::Client) internal object.

The config flags, according to `REST::Client::new` specs, are:

- host

    The host at which _Schema Registry_ is listening.

    The default is [http://localhost:8081](http://localhost:8081)

- timeout

    A timeout in seconds for requests made with the client.  After the timeout the
    client will return a 500.

    The default is 5 minutes.

- cert

    The path to a X509 certificate file to be used for client authentication.

    The default is to not use a certificate/key pair.

- key

    The path to a X509 key file to be used for client authentication.

    The default is to not use a certificate/key pair.

- ca

    The path to a certificate authority file to be used to verify host
    certificates.

    The default is to not use a certificates authority.

- pkcs12

    The path to a PKCS12 certificate to be used for client authentication.

- pkcs12password

    The password for the PKCS12 certificate specified with 'pkcs12'.

- follow

    Boolean that determins whether REST::Client attempts to automatically follow
    redirects/authentication.  

    The default is false.

- useragent

    An [LWP::UserAgent](https://metacpan.org/pod/LWP::UserAgent) object, ready to make http requests.  

    REST::Client will provide a default for you if you do not set this.

### add\_schema( %params )

Registers a new schema version under a subject.

Returns the generated id for the new schema or a RESTful error.

Params keys are:

- SUBJECT ($scalar)

    the name of the Kafka topic

- TYPE ($scalar)

    the type of schema ("key" or "value")

- SCHEMA ($hashref)

    the schema to add

# TODO

...

# AUTHOR

Alvaro Livraghi, <alvarol@cpan.org>

# COPYRIGHT

Copyright 2008 - 2010 by Alvaro Livraghi

This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.
