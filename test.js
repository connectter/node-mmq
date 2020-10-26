'use strict';
const { MongoClient } = require('mongodb');
const { MMQ, Worker} = require('./index');
const client = new MongoClient('mongodb://localhost:27017', { useNewUrlParser: true, useUnifiedTopology: true });
const mmq1 = new MMQ({ client, servicename: 'auth', channel: 'test', dbname: 'connectter'});
const mmq2 = new MMQ({ client, servicename: 'matching', channel: 'test', dbname: 'connectter'});


async function main() {
    (await mmq1.connect());
    (await mmq2.connect());
    
    for (let i = 1; i < 2; i++) {
        (await mmq1.send({ service: '*', event: 'worked', retry: 15, data: { message: 'okeyyyy' } }));
    }

    setTimeout(() => (mmq1.send({ service: '*', event: 'worked', retry: 15, data: { message: 'okeyyyy letsgo' } })), 3000);
    let worker = new Worker({ MMQI: mmq2, shift: true, maxWaitSeconds: 10 });
    worker.on('worked', 'auth', data => {
        console.log(data);
    });
    
    worker.start();
}

main()