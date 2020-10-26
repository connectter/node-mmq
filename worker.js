'use strict';
const _ = require('lodash');
const sleep = require('./helpers/sleep');

class Worker {
    constructor({ MMQI, shift = true, maxWaitSeconds = 600 }) {
        this.mmqi = MMQI;
        this.listeners = [];
        this.send = this.mmqi.send.bind(this.mmqi);
        this.shift = shift;
        this.resolve = null;
        this.maxWaitSeconds = 1000 * maxWaitSeconds;
        this.waitSeconds = 0;
        this.intervalcb = () => {
            this.waitSeconds++;
            if (this.waitSeconds >= this.maxWaitSeconds) {
                if (typeof this.resolve === 'function') {
                    this.resolve.call(this);
                }

                this.waitSeconds = 0;
            }
        }
        this.setInterval = setInterval.bind(this, this.intervalcb, 1);
        this.clearInterval = clearInterval.bind(this);
        this.iid = 0;
        this.empty = false;
    }

    on(...params) {
        if (params.length === 2) {
            this.listeners.push({ event: params[0], cb: params[1] });
        }

        if (params.length === 3) {
            this.listeners.push({ event: params[0], sender: params[1], cb: params[2] });
        }

        return this;
    }

    off(...params) {
        if (params.length === 2) {
            _.remove(this.listeners, listener => listener.event === params[0] && listener.cb === params[1]);
        }

        if (params.length === 3) {
            _.remove(this.listeners, listener => listener.event === params[0] && listener.sender === params[1] && listener.cb === params[2]);
        }

        return this;
    }

    async start() {
        this.mmqi.resolver({ rcb: () => this.resolve });
        while (true) {
            let events = _.uniq(_.map(this.listeners, listener => listener.event));
            let senders = _.uniq(_.compact(_.map(this.listeners, listener => listener.sender || null)));
            let filter = { events, shift: this.shift };
            if (senders.length > 0) filter.senders = senders;
            if (this.empty) {
                (this.iid = this.setInterval.call(this));
                (await new Promise(resolve => this.resolve = resolve));
            }
            this.resolve = null;
            this.clearInterval.call(this, this.iid);
            let { value } = (await this.mmqi.next(filter));
            if (!value) {
                this.empty = true;
                continue;
            }

            this.empty = false;
            for (let listener of this.listeners) {
                let condition = (
                    (
                        (
                            listener.event instanceof RegExp
                            &&
                            listener.event.test(value.event)
                        )
                        ||
                        value.event === listener.event
                    )
                    &&
                    (
                        !listener.sender
                        ||
                        (
                            (
                                listener.sender instanceof RegExp
                                &&
                                listener.sender.test(value.sender)
                            )
                            ||
                            value.sender === listener.sender
                        )
                    )
                );
                if (condition) {
                    let retrynum = 0;
                    while (true) {
                        try {
                            let cbr = listener.cb.call(this, value);
                            if (cbr instanceof Promise) await cbr;
                            break;
                        } catch (error) {
                            if (value.retry > retrynum++) {
                                await sleep(10);
                                continue;
                            }

                            await this.mmqi.log({ sender: value.sender, event: value.event, data: value.data, message: serror.message });
                            await this.send({ service: value.receiver, event: value.event, retry: 0, status: 2, data: value.data });
                            break;
                        }
                    }
                }
            }
        }
    }
}

module.exports = Worker;