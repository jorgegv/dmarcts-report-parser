#!/bin/bash
for i in \
  IO::Socket::SSL \
  CPAN \
  CPAN::DistnameInfo \
  File::MimeInfo \
  IO::Compress::Gzip \
  Getopt::Long \
  Mail::IMAPClient \
  Mail::Mbox::MessageParser \
  MIME::Base64 \
  MIME::Words \
  MIME::Parser \
  MIME::Parser::Filer \
  XML::Parser \
  XML::Simple \
  DBI \
  DVEEDEN/DBD-mysql-4.052.tar.gz \
  DBD::Pg \
  Socket \
  Socket6 \
  PerlIO::gzip \
  ; do
  cpanm install $i --self-contained
done
