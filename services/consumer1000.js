const kafkaMessaging = require("../connections/kafka");

// const SnappyCodec = require("kafkajs-snappy");

// CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const start = async () => {

    const consumer = await kafkaMessaging.initConsumer();
    const producer = await kafkaMessaging.initProducer();

    await consumer.connect();

    await consumer.subscribe({
        topic: "omni.192.9.200.234.envq1",
        fromBeginning: true
    });

    await consumer.run({

        eachBatchAutoResolve: false,

        eachBatch: async ({
            batch,
            resolveOffset,
            heartbeat,
            commitOffsetsIfNecessary
        }) => {

            const outgoingMessages = [];

            for (const message of batch.messages) {

                try {

                    const data = JSON.parse(
                        message.value.toString()
                    );

                    const dtls = data.dtls?.[0];

                    if (
                        dtls?.actn === 1000 &&
                        dtls?.stat === 0
                    ) {

                        console.log(
                            `Received actn=1000 cseq=${data.hdr?.cseq}`
                        );

                        // Delay 10 seconds
                        await new Promise(resolve =>
                            setTimeout(resolve, 10000)
                        );

                        const response = {

                            hdr: {
                                hash: data.hdr.hash,
                                mtyp: data.hdr.mtyp,
                                cseq: data.hdr.cseq,
                                call: data.hdr.call
                            },

                            dtls: [
                                {
                                    actn: 1000,
                                    chnl: 0,
                                    stat: 1,
                                    evnt: 27,
                                    pstr: new Date().toISOString(),
                                    pend: "",
                                    extk: "#",
                                    trmk:
                                        "PlayFIle Added SuccessFully...."
                                }
                            ]
                        };

                        outgoingMessages.push({
                            value: JSON.stringify(response)
                        });
                    }

                    resolveOffset(message.offset);

                } catch (err) {

                    console.error(
                        "Processing Error:",
                        err
                    );
                }
            }

            if (outgoingMessages.length) {

                await producer.send({
                    topic: batch.topic,
                    messages: outgoingMessages
                });

                console.log(
                    `Republished ${outgoingMessages.length} actn=1000 responses`
                );
            }

            await commitOffsetsIfNecessary();
            await heartbeat();
        }
    });

    console.log("Consumer1000 Started...");
};

start().catch(console.error);