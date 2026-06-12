
const kafkaMessaging = require("../connections/kafka");
const { CompressionTypes, CompressionCodecs } = require("kafkajs");
const SnappyCodec = require("kafkajs-snappy");

// Register the Snappy codec
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const start = async () => {

    const consumer =
        await kafkaMessaging.initConsumer(
            "consumer500-testv1"
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

                        const data = JSON.parse(message.value.toString());

                        const dtls =data.dtls?.[0];
                        // const callid = data.hdr?.callid?.toString() || '';

                        if (dtls?.actn === 500 && dtls?.stat === 0) {

                            const cseq = Number(
                                data.hdr?.cseq
                            );
                            const chnl = dtls.chnl;

                            if (
                                Number.isNaN(cseq)
                            ) {

                                console.error(
                                    "Invalid cseq:",
                                    data.hdr?.cseq
                                );

                                resolveOffset(
                                    message.offset
                                );

                                continue;
                            }

                            // Modify original message
                            data.dtls[0].stat = 1;

                            data.dtls[0].expr =
                                new Date().toISOString() + 30000; // 30 seconds later
                            data.dtls[0].anst=
                                new Date().toISOString();
                            data.dtls[0].chnl = chnl;

                            // Route based on cseq % 10
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
                                    data
                                )
                            });

                            console.log(
                                `Prepared cseq=${cseq} -> ${targetTopic}`
                            );
                        }

                        resolveOffset(
                            message.offset
                        );

                    } catch (err) {

                        console.error(
                            "Message processing error:",
                            err
                        );
                    }
                }

                // Publish grouped messages
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
                        `Republished ${messages.length} messages to ${topic}`
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
        "Consumer500 Started..."
    );
};

start().catch((error) => {

    console.error(
        "Consumer startup failed:",
        error
    );
});