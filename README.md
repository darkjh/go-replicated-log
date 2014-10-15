go-replicated-log
==========

This project is a simple paxos-based replicated log implementation,
mainly for learning purpose.

It features a basic paxos protocol and a replicated log service built
on top of it, with various test cases. Inspired by Jepsen, network
partitions are simulated using `iptables`.

## Limits ##
  - No special optimization, such as Multi-Paxos
  - No disk persistance, so does not tolerate replica crash

## TODOs ##
  - Documentation
  - Controllable logging

Code is adapted from my take on the wonderful MIT 6.824 course.
