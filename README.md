Installation
============

Install node.js if needed:

    $ sudo apt-get install nodejs

If you don't have npm installed already:

    $ cd /tmp 
    $ git clone http://github.com/isaacs/npm.git 
    $ cd npm 
    $ sudo make install

Then install a bunch of libs we use:

    $ npm install express jqtpl bufferedstream nodeunit dnode

Unit Tests
==========

There are a set of unit tests with partial code coverage that can be run with nodeunit but these are not without their own set of problems. dnode doesn't completely cleanup after itself, even if you call dnode.close() or dnode.end() so while the unit tests themselves pass, nodeunit will never complete. I consider this a bug in dnode but I haven't gotten around to fixing it yet.

OpenSSL Configuration
=====================

You're free to create SSL certificates however you like. The following instructions are only a guide and are based on instructions from https://help.ubuntu.com/community/OpenSSL.

Certificate Authority
---------------------
Users of the network are authenticated via an SSL certificate chain of trust. 
The administrator of the network will have to create a Certificate Authority certificate and sign each participant's certificates in order for them to take part in the network.
The Certificate Authorities *public* certificate should also be shared with all participants.

The following steps are *only applicable to the network administrator.*

    $ mkdir -p myCA/signedcerts && mkdir -p myCA/private && cd myCA && echo '01' > serial && touch index.txt
    $ vi caconfig.cnf

The contents of this file should be something like:

    # RawDish sample caconfig.cnf file.
    #
    [ ca ]
    default_ca      = local_ca
    #
    #
    # Default location of directories and files needed to generate certificates.
    #
    [ local_ca ]
    dir             = /home/*<username>*/myCA
    certificate     = $dir/cacert.pem
    database        = $dir/index.txt
    new_certs_dir   = $dir/signedcerts
    private_key     = $dir/private/cakey.pem
    serial          = $dir/serial
    #       
    #
    # Default expiration and encryption policies for certificates.
    #
    default_crl_days        = 365
    default_days            = 1825
    default_md              = sha1
    #       
    policy          = local_ca_policy
    x509_extensions = local_ca_extensions
    #       
    #
    # Default policy to use when generating server certificates.  The following
    # fields must be defined in the server certificate.
    #
    [ local_ca_policy ]
    commonName              = supplied
    stateOrProvinceName     = supplied
    countryName             = supplied
    emailAddress            = supplied
    organizationName        = supplied
    organizationalUnitName  = supplied
    #       
    #
    # x509 extensions to use when generating server certificates.
    #
    [ local_ca_extensions ]
    basicConstraints        = CA:false
    nsCertType              = server,client
    #       
    #
    # The default root certificate generation policy.
    #
    [ req ]
    default_bits    = 2048
    default_keyfile = /home/*<username>*/myCA/private/cakey.pem
    default_md      = sha1
    #       
    prompt                  = no
    distinguished_name      = root_ca_distinguished_name
    x509_extensions         = root_ca_extensions
    #
    #
    # Root Certificate Authority distinguished name.  Change these fields to match
    # your local environment!
    #
    [ root_ca_distinguished_name ]
    commonName              = *<username>*'s RawDish CA
    stateOrProvinceName     = NSW
    countryName             = AU
    emailAddress            = *<username>@gmail.com*
    organizationName        = *<username>*'s RawDish CA
    organizationalUnitName  = Friends and Family
    #       
    [ root_ca_extensions ]
    basicConstraints        = CA:true
    
Continuing on:

    $ OPENSSL_CONF=`pwd`/caconfig.cnf openssl req -x509 -newkey rsa:2048 -out cacert.pem -outform PEM -days 1825

Done!

Participant Certificates
------------------------

Each user should create their own certificate and send CSR to the administrator for signing as follows:

    $ mkdir cert && cd cert
    $ vi mynode.cnf

This file should contain:

    #
    # mynode.cnf
    #

    [ req ]
    prompt                  = no
    distinguished_name      = server_distinguished_name

    [ server_distinguished_name ]
    commonName              = my.dyndns.address:<port>
    stateOrProvinceName     = QLD
    countryName             = AU
    emailAddress            = queenslander@examplemail.com
    organizationName        = John Queenslander
    organizationalUnitName  = Brisbane University

To create the certificate and strip paraphrase from it:

    $ OPENSSL_CONF=`pwd`/mynode.cnf openssl req -newkey rsa:1024 -keyout tempkey.pem -keyform PEM -passout pass:dddd -out tempreq.pem -outform PEM &&  OPENSSL_CONF=../myCA/caconfig.cnf openssl ca -in tempreq.pem -out server_crt.pem &&  openssl rsa -passin pass:dddd < tempkey.pem > server_key.pem

You should now have the files server\_key.pem (never share this with anyone) and tempreq.pem (the request that needs signing with the network administrator's CA).

Signing Participant Certificates
--------------------------------

Each participants certificate must be signed as follows:

    $ cd myCA
    $ OPENSSL_CONF=`pwd`/caconfig.cnf openssl ca -in tempreq.pem -out server_crt.pem

Users should send tempreq.pem to the admin and the admin should return server\_crt.pem
    
HOW TO RUN A STORAGE NETWORK
============================

1. Use OpenSSL to create yourself a certificate authority (see https://help.ubuntu.com/community/OpenSSL).
1. Create yourself a cert and sign it with your CA. Get your friend to do the same, sign theirs with your CA.
   Whenever possible, use your _publically addressable ip address_ for commmon name. This will be where users connect initially to bootstrap the network.
. You each run something like:

    $ node server.js --storage /my/storage/path:100G --storage /my/other/storage/path:500G --keyfile mykey.crt --certfile mycert.crt --peer friend1.crt --peer friend2.crt ...

1. Your node will connect to your friends node, trade known node addresses and repeat until everyone is connected in a full mesh.
1. Connect to https://localhost:8123/ to see the network status or use WebDAV to upload/download your data files.

Note that without your key and certificate, you won't be able to access your data so its *critical* that you keep a copy off-site somewhere safe. I suggest a USB drive entrusted to a close friend / relative.

DESIGN
======

- dnode with TLS is used for an RPC mechanism between nodes.
- express is used with stylus for the local web interface.
- jsDAV is used for WebDAV access to the exposed filesystem (via http://localhost:webport/data/).

To form the network, each node maintains a separate TCP connection for each direction of traffic to every other node. 
The client side of this connection queries remote BlockStores and tell the server about other peers (including itself).
An interval timer is used to round-robin poll the remote BlockStores for their current size. This value is used when we decide where to place new data.

TODO
====

- Block metadata (owner, encoding, hmac, ...)
- Quota enforcement (requires owner information)
- Directories (stored at one of 4 locations, cycled in round-robin fashion with an incrementing counter and random number. Highest counter value wins. Random number used in case of collisions. Must assume our filesystem is mutable for this to work.)
- Block to object mapping (Uses crypto seeded with password or hash of private cert. Result is split into HMACed chunks, duplicated 3x and placed in BlockStores.)
- jsDAV classes for exposing files, directories and quota to the localhost.

