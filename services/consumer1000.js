const kafkaMessaging = require("../connections/kafka");

const { CompressionTypes, CompressionCodecs } = require("kafkajs");
const SnappyCodec = require("kafkajs-snappy");

// Register the Snappy codec
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const start = async () => {

    const consumer =
        await kafkaMessaging.initConsumer(
            "consumer1000-testv1"
        );

    const producer =
        await kafkaMessaging.initProducer();

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

            const topicMessages = {};

            try {

                console.log(
                    `Received batch of ${batch.messages.length} messages`
                );

                for (const message of batch.messages) {

                    try {

                        const data = JSON.parse(
                            message.value.toString()
                        );

                        const dtls =
                            data.dtls?.[0];

                        if (
                            dtls?.actn === 1000 &&
                            dtls?.stat === 0
                        ) {

                            const cseq = Number(
                                data.hdr?.cseq
                            );
                            const chnl = dtls.chnl;

                            console.log(
                                `Received actn=1000 cseq=${cseq}`
                            );

                            const response1 = {

                                hdr: {
                                    hash:
                                        data.hdr.hash,
                                    mtyp:
                                        data.hdr.mtyp,
                                    cseq:
                                        data.hdr.cseq,
                                    call:
                                        data.hdr.call
                                },

                                dtls: [
                                    {
                                        actn: 1000,
                                        chnl: chnl,
                                        stat: 1,
                                        evnt: 27,
                                        pstr:
                                            new Date().toISOString(),
                                        pend: "",
                                        extk: "#",
                                        trmk:
                                            "PlayFIle Added SuccessFully...."
                                    }
                                ]
                            };

                        

                            // Create NEW response JSON
                            const response = {

                                hdr: {
                                    hash:
                                        data.hdr.hash,
                                    mtyp:
                                        data.hdr.mtyp,
                                    cseq:
                                        data.hdr.cseq,
                                    call:
                                        data.hdr.call
                                },

                                dtls: [
                                    {
                                        actn: 1000,
                                        chnl: chnl,
                                        stat: 2,
                                        evnt: 27,
                                        pstr:
                                            new Date().toISOString(),
                                        pend: "",
                                        extk: "#",
                                        trmk:
                                            "PlayFIle Added SuccessFully...."
                                    }
                                ]
                            };

                            const bucket =
                                cseq % 10;

                            const targetTopic =
                                `omni.call.${bucket}`;

                            // Send response1 immediately
                            await producer.send({
                                topic: targetTopic,
                                messages: [
                                    {
                                        key: String(cseq),
                                        value: JSON.stringify(response1)
                                    }
                                ]
                            });

                            console.log(
                                `Sent immediate response1 for cseq=${cseq}`
                            );

                            // Send response after 10 seconds
                            setTimeout(async () => {

                                try {

                                    await producer.send({
                                        topic: targetTopic,
                                        messages: [
                                            {
                                                key: String(cseq),
                                                value: JSON.stringify(response)
                                            }
                                        ]
                                    });

                                    console.log(
                                        `Sent delayed response for cseq=${cseq}`
                                    );

                                } catch (err) {

                                    console.error(
                                        "Delayed send failed:",
                                        err
                                    );
                                }

                            }, 10000);
                        }

                        resolveOffset(
                            message.offset
                        );

                    } catch (err) {

                        console.error(
                            "Processing Error:",
                            err
                        );
                    }
                }

                for (
                    const [
                        topic,
                        messages
                    ] of Object.entries(
                        topicMessages
                    )
                ) {

                    await producer.send({
                        topic,
                        messages
                    });

                    console.log(
                        `Republished ${messages.length} messages to ${topic}  message details: ${JSON.stringify(messages)}`
                    );
                }

                await commitOffsetsIfNecessary();

                await heartbeat();

            } catch (error) {

                console.error(
                    "Batch processing error:",
                    error
                );
            }
        }
    });

    console.log(
        "Consumer1000 Started..."
    );
};

start().catch((error) => {

    console.error(
        "Consumer startup failed:",
        error
    );
});