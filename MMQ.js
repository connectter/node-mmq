'use strict';
const _ = require('lodash');
const sleep = require('./helpers/sleep');

class MMQ {
    constructor({ client, channel, servicename, dbname }) {
        this.channel = channel;
        this.servicename = servicename || 'default';
        this.dbname = dbname || 'MMQ';
        this.qcoll = this.dbname + '_queues';
        this.scoll = this.dbname + '_services';
        this.lcoll = this.dbname + '_logs';
        this.pcoll = this.dbname + '_pubsub'
        this.db = null;
        this.client = client;
        this.pubsubNext = null;
    }

    async connect() {
        if (!this.client.isConnected()) {
            (await this.client.connect());
            this.db = this.client.db(this.dbname);
            let collections = await this.db.listCollections().toArray();
            if (!collections.find(x => x.name === this.scoll)) {
                (await this.db.createCollection(this.scoll));
                (await this.db.collection(this.scoll).createIndex('name'));
            }

            if (!collections.find(x => x.name === this.qcoll)) {
                (await this.db.createCollection(this.qcoll));
                (await this.db.collection(this.qcoll).createIndex('channel'));
                (await this.db.collection(this.qcoll).createIndex('sender'));
                (await this.db.collection(this.qcoll).createIndex('receiver'));
                (await this.db.collection(this.qcoll).createIndex('event'));
                (await this.db.collection(this.qcoll).createIndex('parent'));
                (await this.db.collection(this.qcoll).createIndex('status'));
            }

            if (!collections.find(x => x.name === this.lcoll)) {
                (await this.db.createCollection(this.lcoll));
                (await this.db.collection(this.lcoll).createIndex('sender'));
                (await this.db.collection(this.lcoll).createIndex('receiver'));
                (await this.db.collection(this.lcoll).createIndex('channel'));
                (await this.db.collection(this.lcoll).createIndex('event'));
            }

            if (!collections.find(x => x.name === this.pcoll)) {
                (await this.db.createCollection(this.pcoll, { capped: true, size: 10000000, max: 100000 }));
                (await this.db.collection(this.pcoll).insertOne({ channel: 'test', sender: 'test', receiver: 'test', event: 'test' }));
                (await this.db.collection(this.pcoll).createIndex('channel'));
                (await this.db.collection(this.pcoll).createIndex('sender'));
                (await this.db.collection(this.pcoll).createIndex('receiver'));
                (await this.db.collection(this.pcoll).createIndex('parent'));
                (await this.db.collection(this.pcoll).createIndex('event'));
            }
        }

        if (this.client.isConnected()) {
            this.db = this.client.db(this.dbname);
        }

        (await this.db.collection(this.scoll).updateOne({ name: this.servicename }, { $set: { name: this.servicename } }, { upsert: true }));
        let lastdoc = await this.db.collection(this.pcoll).findOne({ receiver: this.servicename, channel: this.channel }, { sort: [['_id', 'desc']] });
        if (lastdoc) {
            this.pubsubNext = lastdoc._id;
        }
    }

    async log({ sender, event, message, data }) {
        (await this.db.collection(this.lcoll).insertOne({ sender, receiver: this.servicename, channel: this.channel, event, message, data }));
        return true;
    }

    async next({ senders = null, events = null, filters = {}, shift = true }) {
        let filter = {
            receiver: this.servicename,
            channel: this.channel,
            status: 0
        }

        if (events) filter.event = _.isArray(events) ? { $in: events } : events;
        if (senders) filter.sender = _.isArray(senders) ? { $in: senders } : senders;
        if (filters) filter = _.merge(filter, filters);

        let options = {
            sort: [
                [
                    '_id',
                    'asc'
                ]
            ]
        }

        let collection = this.db.collection(this.qcoll);
        return await (shift ? collection.findOneAndDelete(filter, options) : collection.findOneAndUpdate(filter, { $set: { status: 1 } }, options));
    }

    async resolver({ senders = null, events = null, filters = {}, rcb = null }) {
        let filter = {
            receiver: this.servicename,
            channel: this.channel
        }

        let options = {
            tailable: true,
            awaitData: true,
            numberOfRetries: -1
        }

        if (events) filter.event = _.isArray(events) ? { $in: events } : events;
        if (senders) filter.sender = _.isArray(senders) ? { $in: senders } : senders;
        if (filters) filter = _.merge(filter, filters);
        if (this.pubsubNext) filter._id = { $gt: this.pubsubNext };

        let cursor = this.db.collection(this.pcoll).find(filter, options);
        while (true) {
            await cursor.next();
            while (true) {
                if (typeof rcb.call(this) !== 'function') {
                    await sleep(500);
                    continue;
                }
                break;
            }

            let scb = rcb;
            while (true) {
                scb = scb.call(this);
                if (typeof scb !== 'function') {
                    break;
                }
            }
        }
    }

    async send({ service = '*', event, retry = 0, status = 0, data = {}, parent = null, waitReply = false }) {
        if (service === '*') {
            let services = (await this.db.collection(this.scoll).find({}).toArray());
            for (let { name } of services) {
                if (name !== this.servicename) {
                    (await this.db.collection(this.qcoll).insertOne({ channel: this.channel, sender: this.servicename, receiver: name, event, retry, status, data }));
                    (await this.db.collection(this.pcoll).insertOne({ channel: this.channel, sender: this.servicename, receiver: name, event }));
                }
            }

            return { channel: this.channel, sender: this.servicename, receiver: _.map(services, x => x.name), event, retry, status, data };
        }

        let qi = await this.db.collection(this.qcoll).insertOne({ channel: this.channel, sender: this.servicename, receiver: service, event, retry, status, parent, data });
        let pi = await this.db.collection(this.pcoll).insertOne({ channel: this.channel, sender: this.servicename, receiver: service, parent, event });

        if (waitReply) {
            let maxWaitSeconds = 1000 * 60 * 1;
            let waitSeconds = 0;
            let resolve = null;
            let timeout = 20 * 60 * 1000;
            let timeouted = false;
            let intervalcb = () => {
                waitSeconds++;
                if (waitSeconds >= maxWaitSeconds) {
                    if (typeof resolve === 'function') {
                        resolve.call(this);
                    }

                    waitSeconds = 0;
                }
            }

            let _setInterval = setInterval.bind(this, intervalcb, 1);
            let _clearInterval = clearInterval.bind(this);
            let iid = 0;
            setTimeout(() => (timeouted = true), timeout);
            this.resolver({ senders: service, events: event, filters: { parent: pi.insertedId }, rcb: () => resolve });
            while (true) {
                if (timeouted) return null;
                (iid = _setInterval.call(this));
                (await new Promise(r => (resolve = r)));
                resolve = null;
                _clearInterval.call(this, iid);
                let { value } = (await this.next({ senders: service, events: event, filters: { parent: qi.insertedId }, shift: true }));
                if (!value) continue;

                return value;
            }
        }


        return { channel: this.channel, sender: this.servicename, receiver: service, event, retry, status, data };
    }
}

module.exports = MMQ;