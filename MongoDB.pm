#!/usr/bin/perl -w

package DataAbstract::MongoDB;

use MongoDB;
use MongoDB::OID;
use Data::Dumper;
use Exporter;
use boolean;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);
use strict;

$VERSION     = 1.00;
@ISA         = qw(Exporter);
@EXPORT      = ();
@EXPORT_OK   = qw(new get put);
%EXPORT_TAGS = ( DEFAULT => [qw(&new)],
                 Both    => [qw(&new &get &put &delete)]);

## Configuration ##
our $PACKAGE_NAME = 'DataAbstract::MongoDB';
our $DEFAULT_HOST = 'localhost';
our $DEFAULT_PORT = 27017;
###################

#### Methods ####

# Method: new( $dbName, $collectionName )
sub new {
  # Create blessed logs object for new() logging
  my $logs = {};
  bless($logs);

  # Handle Arguments
  my $class = shift;
  my $dbName = shift;
  my $collectionName = shift;
  my $options = shift;

  if( !defined($dbName) ) {
    $logs->logger( 'FATAL' , "${PACKAGE_NAME}::new() database argument required" );
    return undef;
  }
  if( !defined($collectionName) ) {
    $logs->logger( 'FATAL' , "${PACKAGE_NAME}::new() collection argument required" );
    return undef;
  }

  # Check for Host and Port
  my $host = $DEFAULT_HOST;
  if( defined($options->{host}) ) { $host = $options->{host}; }
  my $port = $DEFAULT_PORT;
  if( defined($options->{port}) ) { $port = $options->{port}; }

  # Check Authentication Information
  my $dbAuth = 0;
  if( defined($options->{user}) && defined($options->{pass}) ) { $dbAuth = 1; }

  # Create Client Connection
  my $client = MongoDB::Connection->new( 'host' => "mongodb://$host:$port" ) || 
  			$logs->logger( 'FATAL' , "${PACKAGE_NAME}::new() failed to connect to mongodb://$host:$port : $!");

  # Authenticate against Client if required
  if( $dbAuth ) {
    my $return = $client->authenticate( $dbName, $options->{user}, $options->{pass} ); 
    if( ref $return eq '' ) {
      $logs->logger( 'FATAL' , "${PACKAGE_NAME}::new() $return for user '".$options->{user}."' on mongodb://$host:$port/$dbName");
      return 0;
    }
  }

  # Connect to DB
  my $db = $client->get_database( $dbName ) ||
  			$logs->logger( 'FATAL' , "${PACKAGE_NAME}::new() failed to connect to $dbName on mongodb://$host:$port : $!");

  # Create Class Object
  my $self = {
      CLIENT		=> $client,
      DB		=> $db,
      COLLECTION_NAME	=> $collectionName,
      COLLECTION	=> $db->get_collection( $collectionName ),
      QUERY		=> undef,
      ATTRS		=> undef,
      UPDATE		=> undef,
      DELETE		=> undef,
      ERRORS		=> $logs->{ERRORS},
      LAST_ERROR	=> $logs->{LAST_ERROR},
    };
  bless( $self );
  return $self;
}

# Method: disconnect()
sub disconnect {

}

# Method: debug()
sub debug {
  my $self = shift @_;
  $self->{DEBUG} = 1;
}

# Method: get( \@criteria, <\%attrs> )
sub get {
  my $self = shift @_;
  my $criteriaArg = shift @_;
  my $attrsArg = shift @_;

  # Format Query for Mongo Query
  my $criteria = $criteriaArg;

  # Format Attributes for Mongo Query
  my $attrs = {};
  foreach my $attr ( @$attrsArg ) {
    $attrs->{$attr} = 1;
  }
  
  # Debug
  logger( 'DEBUG' , "${PACKAGE_NAME}::get() objects from '".$self->{COLLECTION_NAME}."':\n  criteria=".Dumper($criteria)."  attrs=".Dumper($attrs) ) if( defined($self->{DEBUG}) && $self->{DEBUG} );

  my $cursor = $self->{COLLECTION}->find( $criteria, $attrs ) || 
  		logger( 'FATAL' , "${PACKAGE_NAME}::get() query against '".$self->{COLLECTION_NAME}."' failed: ".$self->{DB}->last_error({w => 2}) );
  #my $cursor = $collection->query( $query )->limit( # )->skip( # )->sort( \%criteria );

  # Iterating Results
  my @results;
  while( my $doc = $cursor->next ) {
    #if( defined($doc->{_id}) && ref $doc->{_id} ne '' ) {
    #  my $id = $doc->{_id}->to_string;
    #  $doc->{_id} = $id;
    #}
    push( @results, $doc );
  }

  # Debug
  logger( 'DEBUG' , "${PACKAGE_NAME}::get() results:".Dumper(@results) ) if( defined($self->{DEBUG}) && $self->{DEBUG} );

  return \@results;
}

# Method: put( \%criteria, %object ), returns BOOL
sub put {
  my $self = shift @_;
  my $criteriaArg = shift @_;
  my $objectArg = shift @_;

  # Format Query for Mongo Query
  my $criteria = $criteriaArg;

  #my @results;
  ## Insert many objects
  #if( ref $objectArg eq 'ARRAY' ) {
  #  @results = $self->{COLLECTION}->batch_insert($objectArg, {'safe' => 1} );
  ## Insert one object
  #} elsif( ref $objectArg eq 'HASH' ) {
  #  push( @results, $self->{COLLECTION}->insert($objectArg, {'safe' => 1} ) );
  #} else {
  #  return;
  #}

  # Make sure object is formatted properly
  my $object = {};
  foreach my $attr (keys %$objectArg) {
    my $value = $objectArg->{$attr};
    if( !($value =~ /^\s*$/) ) {
      $object->{$attr} = $value;
    }
  }

  # upsert
  my $results = $self->{COLLECTION}->update($criteria, $object, {'upsert' => true, 'safe' => true }) || 
  		logger( 'FATAL' , "${PACKAGE_NAME}::put() upserting object into '".$self->{COLLECTION_NAME}."' failed: ".$self->{DB}->last_error({w => 2}));

  return $results;
}

# Method: remove( \@criteria ), returns BOOL
sub remove {
  my $self = shift @_;
  my $criteriaArg = shift @_;

  # Format Query for Mongo Query
  my $criteria = $criteriaArg;

  # Delete
  my $result = $self->{COLLECTION}->remove( $criteria ) ||
  		logger( 'FATAL' , "${PACKAGE_NAME}::remove() removing object from '".$self->{COLLECTION_NAME}."' failed: ".$self->{DB}->last_error({w => 2}) );

  return $result;
}

# Method: get_indexes(), returns \%indexes
sub get_indexes {
  my $self = shift @_;

  return $self->{COLLECTION}->get_indexes;
}

# Method: set_indexes( \%indexes ), returns BOOL
sub set_indexes {
  my $self = shift @_;
  my $indexArg = shift @_;

  my $return = $self->{COLLECTION}->ensure_index( $indexArg, { unique => true, safe => true } );
  if( !defined($return) ) { $return = 1; }
  elsif( $return eq 0 ) { logger( 'FATAL' , "${PACKAGE_NAME}::index() indexing '".$self->{COLLECTION_NAME}."' failed: ".$self->{DB}->last_error({w => 2}) ); }

  return $return;
}

# Method: get_count( \%criteria ), returns COUNT
sub get_count {
  my $self = shift @_;
  my $criteriaArg = shift @_;

  # Format Query for Mongo Query
  my $criteria = {};
  if( defined($criteriaArg) ) { $criteria = $criteriaArg; }

  my $count = $self->{COLLECTION}->count( $criteria ); 
  if( !defined($count) ) {
    $self->logger( 'FATAL' , "${PACKAGE_NAME}::count() retrieving count from '".$self->{COLLECTION_NAME}."' failed: ".Dumper($self->{DB}->last_error({w => 2})) );
    $count = -1;
  }

  return $count;
}

# Method: get_id( ${MONGO_DB::OID} ), return STRING
sub get_id {
  my $self = shift @_;
  my $oid = shift @_;

  return $oid->to_string;
}

# Method: get_id_date( ${MONGO_DB::OID} ), returns STRING
sub get_id_date {
  my $self = shift @_;
  my $oid = shift @_;

  return DateTime->from_epoch(epoch => $oid->get_time);
}


#### Functions ####

# Function: logger()
sub logger {
  my $self = shift @_;
  my $logLevel = shift @_;
  my $message = shift @_;

  # Add error to @ERRORS and $LAST_ERROR
  push( @{$self->{ERRORS}}, "$logLevel $message" );
  $self->{LAST_ERROR} = "$logLevel $message";

  if( $logLevel eq 'INFO' ) {
    warn "$logLevel $message\n";
  }
  if( $logLevel eq 'WARN' ) {
    warn "$logLevel $message\n";
  }
  if( $logLevel eq 'FATAL' ) {
    warn "$logLevel $message\n";
  }
  if( $logLevel eq 'DEBUG' ) {
    warn "$logLevel $message\n";
  }
}

1;
