### Updating Information for BCPC clusters

These instructions assume that you basically know what you are doing.  =)

#### 20130817

Due to a change in Vagrant 1.2.5+, the default bootstrap node cannot have its
IP end in .1 (ie, 10.0.100.1).  In the current Vagrant scripts, the automated
bootstrap node will be 10.0.100.3.  (The host machine stays at 10.0.100.2.)

The default Ceph version has been upgraded from Cuttlefish (0.61) to Dumpling
(0.67).

Follow rolling upgrade notes described in:
 http://ceph.com/docs/master/release-notes/#v0-67-dumpling

* Upgrade ceph-common on all nodes that will use the command line `ceph` utility.
* Upgrade all monitors (upgrade ceph package, restart ceph-mon daemons). This can happen one daemon or host at a time. Note that because cuttlefish and dumpling monitors can't talk to each other, all monitors should be upgraded in relatively short succession to minimize the risk that an a untimely failure will reduce availability.
* Upgrade all osds (upgrade ceph package, restart ceph-osd daemons). This can happen one daemon or host at a time.
* Upgrade radosgw (upgrade radosgw package, restart radosgw daemons).

In Dumpling, the ``client.bootstrap-osd`` cephx key has new capabilities (see
``ceph auth list``).  Cuttlefish required the following monitor capabilities:

```
mon 'allow command osd create ...; allow command osd crush set ...; allow command auth add * osd allow\\ * mon allow\\ rwx; allow command mon getmap' 
```

In Dumpling, it is now reduced to:
```
mon 'allow profile bootstrap-osd' 
```

BCPC now uses the standard Opscode Community ntp cookbook instead of rolling
our own ntp recipes.  Environments need to be updated from:

```
"bcpc": {
  ...
  "ntp_servers" : [ "0.pool.ntp.org", "1.pool.ntp.org", "2.pool.ntp.org", "3.pool.ntp.org" ],
  ...
}
```

to (at the outer level - akin to ``bcpc``, ``chef_client``, and ``ubuntu``):

```
"ntp": {
   "servers" : [ "0.pool.ntp.org", "1.pool.ntp.org", "2.pool.ntp.org", "3.pool.ntp.org" ]
},
```

#### 20130720
Kibana has been upgraded from version 2 to version 3.
First run `service stop kibana` on each headnode before running
chef-client with the updated recipe.