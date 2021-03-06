#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time

def consumer():
    r = ''
    while True:
        n = yield r
        if not n:
            return
        time.sleep(5)
        print('[CONSUMER] Consuming %s...' % n)
        r = '%s OK' %n

def produce(c):
    c.send(None)
    n = 0
    while n < 5:
        n = n + 1
        print('[PRODUCER] Producing %s...' % n)
        r = c.send(n)
        print('[PRODUCER] Consumer return: %s' % r)
    c.close()

c = consumer()
produce(c)