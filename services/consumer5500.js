const kafkaMessaging = require("../connections/kafka");

const { CompressionTypes, CompressionCodecs } = require("kafkajs");
const SnappyCodec = require("kafkajs-snappy");

// Register the Snappy codec
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const start = async () => {

    const consumer =
        await kafkaMessaging.initConsumer(
            "consumer5500-testv1"
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
                            dtls?.actn === 5500 &&
                            dtls?.stat === 0
                        ) {

                            const cseq = Number(
                                data.hdr?.cseq
                            );
                            const chnl = dtls.chnl;

                            // console.log(
                            //     `Received actn=5500 cseq=${cseq}`
                            // );

                            const cetm = new Date().toISOString();

                            const response = {
                                hdr: {
                                    hash: data.hdr.hash,
                                    mtyp: data.hdr.mtyp,
                                    cseq: data.hdr.cseq,
                                    call: data.hdr.call
                                },
                                dtls: [
                                    {
                                        actn: 5500,
                                        stat: 1,
                                        evnt: 3,
                                        hupd: [
                                            {
                                                chnl: Number(dtls?.chnl?.[0] ?? 0),
                                                cetm
                                            }
                                        ]
                                    }
                                ]
                            };



                            const bucket =
                                cseq % 10;

                            const targetTopic =
                                `omni.call.${bucket}`;

                            if (
                                !topicMessages[
                                targetTopic
                                ]
                            ) {

                                topicMessages[
                                    targetTopic
                                ] = [];
                            }

                            topicMessages[
                                targetTopic
                            ].push({

                                key: String(cseq),

                                value: JSON.stringify(
                                    response
                                )
                            });

                            // console.log(
                            //     `Prepared cseq=${cseq} -> ${targetTopic}`
                            // );
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
                        `Republished ${messages.length} messages to ${topic} `
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
        "Consumer5500 Started..."
    );
};

start().catch((error) => {

    console.error(
        "Consumer startup failed:",
        error
    );
});