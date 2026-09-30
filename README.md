# Kafka (NestJS Microservices)

**Source:** https://docs.nestjs.com/microservices/kafka

[Kafka](https://kafka.apache.org/) হলো একটা open source, distributed streaming platform, যার তিনটা মূল ক্ষমতা আছে:

- Message queue বা enterprise messaging system এর মতো, record এর stream publish আর subscribe করা।
- Record এর stream গুলো একটা fault-tolerant, durable উপায়ে store করা।
- Record এর stream গুলো ঘটার সাথে সাথে process করা।

Kafka project এর লক্ষ্য হলো real-time data feed handle করার জন্য একটা unified, high-throughput, low-latency platform দেওয়া। এটা Apache Storm আর Spark এর সাথে integrate হয়ে real-time streaming data analysis এর কাজ করে।

---

## 1. Installation

Kafka-based microservice বানানো শুরু করার আগে, প্রথমে প্রয়োজনীয় package টা install করো:

```bash
npm i --save kafkajs
```

---

## 2. Overview

অন্য Nest microservice transport layer implementation এর মতোই, `createMicroservice()` method এ পাস করা options object এর `transport` property দিয়ে Kafka transporter বেছে নাও, সাথে একটা optional `options` property, নিচের মতো:

```typescript
const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
  transport: Transport.KAFKA,
  options: {
    client: {
      brokers: ['localhost:9092'],
    },
  },
});
```

> **Hint:** `Transport` enum টা `@nestjs/microservices` package থেকে import করা হয়।

---

## 3. Options

`options` property টা বেছে নেওয়া transporter অনুযায়ী নির্দিষ্ট। **Kafka** transporter নিচের property গুলো expose করে।

| Property | বিবরণ |
|---|---|
| `client` | Client configuration option (দেখো [KafkaJS client configuration](https://kafka.js.org/docs/configuration)) |
| `consumer` | Consumer configuration option (দেখো [KafkaJS consumer options](https://kafka.js.org/docs/consuming#a-name-options-a-options)) |
| `run` | Consumer এর `run()` method এ পাস করা option (দেখো [KafkaJS consuming messages](https://kafka.js.org/docs/consuming)) |
| `subscribe` | Consumer এর `subscribe()` method এ পাস করা option, `topics` বাদে — যেটা Nest নিজেই registered pattern গুলো থেকে বের করে নেয় (দেখো [KafkaJS fromBeginning](https://kafka.js.org/docs/consuming#frombeginning)) |
| `producer` | Producer configuration option (দেখো [KafkaJS producer options](https://kafka.js.org/docs/producing#options)) |
| `send` | Producer এর `send()` method এ পাস করা option, `topic` আর `messages` বাদে (দেখো [KafkaJS producing messages](https://kafka.js.org/docs/producing#options)) |
| `producerOnlyMode` | শুধু client এর জন্য। Consumer group registration skip করে, যাতে client শুধু একটা producer হিসেবেই কাজ করে (`boolean`) |
| `postfixId` | `clientId` আর `groupId` value এর শেষে যোগ হওয়া suffix। Server এ default `'-server'`, আর client এ default `'-client'` (`string`) |

---

## 4. Client

অন্য microservice transporter থেকে Kafka একটু ভিন্ন: `ClientProxy` type এর বদলে, এখানে `ClientKafkaProxy` ব্যবহার করা হয়।

অন্য microservice transporter এর মতোই, একটা `ClientKafkaProxy` instance তৈরি করার জন্য তোমার কাছে [বেশ কিছু option](https://docs.nestjs.com/microservices/basics#client) আছে।

একটা option হলো `ClientsModule`। এটা import করো এবং এর `register()` method ব্যবহার করে উপরে `createMicroservice()` method এ দেখানো একই property গুলো সহ একটা options object পাস করো, সাথে injection token হিসেবে ব্যবহারের জন্য একটা `name` property। `ClientsModule` সম্পর্কে আরো জানতে [microservices overview এর client section](https://docs.nestjs.com/microservices/basics#client) দেখো।

```typescript
@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'HERO_SERVICE',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'hero',
            brokers: ['localhost:9092'],
          },
          consumer: {
            groupId: 'hero-consumer',
          },
        },
      },
    ]),
  ],
  // ...
})
```

`ClientProxyFactory` অথবা `@Client()` decorator দিয়েও client তৈরি করা যায়, দুটোই [microservices overview](https://docs.nestjs.com/microservices/basics#client) এ describe করা আছে।

`@Client()` decorator নিচের মতো ব্যবহার করো:

```typescript
@Client({
  transport: Transport.KAFKA,
  options: {
    client: {
      clientId: 'hero',
      brokers: ['localhost:9092'],
    },
    consumer: {
      groupId: 'hero-consumer',
    },
  },
})
client: ClientKafkaProxy;
```

---

## 5. Message Pattern

Kafka microservice message pattern এ request আর reply channel এর জন্য দুটো topic ব্যবহার হয়। `ClientKafkaProxy.send()` method একটা [correlation id](https://www.enterpriseintegrationpatterns.com/patterns/messaging/CorrelationIdentifier.html), reply topic, আর reply partition কে request message এর সাথে associate করে, এভাবে message পাঠায় [return address](https://www.enterpriseintegrationpatterns.com/patterns/messaging/ReturnAddress.html) সহ। এর জন্য একটা message পাঠানোর আগে `ClientKafkaProxy` instance টাকে reply topic এ subscribe করা থাকতে হবে, এবং অন্তত একটা partition এ assign থাকতে হবে।

ফলে, প্রতিটা running Nest application এর জন্য অন্তত একটা reply topic partition লাগবে। যেমন, তুমি যদি ৪টা Nest application run করো কিন্তু reply topic এ শুধু ৩টা partition থাকে, তাহলে একটা Nest application message পাঠানোর চেষ্টা করার সময় fail করবে।

নতুন `ClientKafkaProxy` instance চালু হলে, সেগুলো consumer group এ join করে এবং তাদের নিজস্ব topic এ subscribe করে। এটা group এর consumer গুলোকে assign করা topic partition এর একটা rebalance trigger করে।

সাধারণত, topic partition গুলো round-robin assigner দিয়ে assign করা হয়, যেটা consumer name অনুযায়ী sort করা consumer এর collection এ topic partition assign করে — এই নাম গুলো application চালু হওয়ার সময় randomly সেট হয়। তবে, যখন একটা নতুন consumer consumer group এ join করে, সেটা ওই collection এ যেকোনো জায়গায় position হতে পারে। নতুন consumer এর পরে position করা আগে থেকে থাকা consumer গুলো তখন ভিন্ন partition assign পেতে পারে, আর সেই consumer গুলো rebalance এর আগে পাঠানো request এর response message হারিয়ে ফেলে।

`ClientKafkaProxy` consumer গুলোকে response message হারানো থেকে বাঁচাতে, Nest একটা built-in custom partition assigner ব্যবহার করে। একটা rebalance এর সময়, এটা প্রতিটা existing consumer কে তার আগে assign করা reply partition ধরে রাখতে দেয়, আর বাকি partition গুলো অন্য consumer দের মধ্যে ভাগ করে দেয়।

---

## 6. Regular Expression Pattern

KafkaJS regular expression দিয়ে topic এ subscribe করা support করে। NestJS v12 থেকে, `@MessagePattern()` বা `@EventPattern()` এ সরাসরি একটা `RegExp` পাস করা যায়। Nest এই pattern টা রেখে দেয়, KafkaJS এর `subscribe()` call এ forward করে, আর incoming topic এর জন্য একটা handler resolve করার সময় regular expression matching এর উপর নির্ভর করে।

```typescript
@EventPattern(/^hero\..+$/)
handleHeroEvents(@Payload() data: any, @Ctx() context: KafkaContext) {
  console.log(`Topic: ${context.getTopic()}`);
}
```

এটা handler টাকে এই expression এর সাথে মিলে যাওয়া প্রতিটা topic এর জন্য subscribe করে দেয় (`hero.kill.dragon`, `hero.rescue.villager`, ইত্যাদি) — প্রতিটা আলাদা করে explicitly register না করেই। কোন topic দিয়ে message এসেছে সেটা জানতে `context.getTopic()` ব্যবহার করো।

> **Hint:** Regular expression pattern হলো একটা Kafka-specific ক্ষমতা; অন্য transporter গুলো এখনো exact value দিয়ে pattern match করে। Nest matching করার আগে `lastIndex` reset করে দেয়, তাই global (`/g`) আর sticky (`/y`) expression গুলোতে stateful miss হয় না।

---

## 7. Message Response Subscription

> **Note:** এই section টা শুধু তখনই প্রাসঙ্গিক, যদি তুমি [request-response](https://docs.nestjs.com/microservices/basics#request-response) message style ব্যবহার করো (`@MessagePattern()` decorator আর `ClientKafkaProxy.send()` method)। [Event-based](https://docs.nestjs.com/microservices/basics#event-based) communication এ (`@EventPattern()` decorator আর `ClientKafkaProxy.emit()` method) response topic এ subscribe করার দরকার নেই।

`ClientKafkaProxy` একটা `subscribeToResponseOf()` method দেয়, যেটা একটা request এর topic name argument হিসেবে নেয় এবং সেই থেকে derive করা reply topic name কে reply topic এর একটা collection এ add করে। Message pattern implement করার সময় এই method টা প্রয়োজন।

```typescript
onModuleInit() {
  this.client.subscribeToResponseOf('hero.kill.dragon');
}
```

যদি `ClientKafkaProxy` instance টা asynchronously তৈরি করা হয়, তাহলে `connect()` method call করার আগে `subscribeToResponseOf()` method call করো।

```typescript
async onModuleInit() {
  this.client.subscribeToResponseOf('hero.kill.dragon');
  await this.client.connect();
}
```

---

## 8. Incoming Message

Nest, incoming Kafka message গুলোকে `key`, `value`, আর `headers` property সহ object হিসেবে receive করে, যাদের value গুলো `Buffer` type এর। Nest এই buffer গুলোকে string এ convert করে value গুলো parse করে। যদি একটা string "object-like" হয় (অর্থাৎ, একটা curly brace বা square bracket দিয়ে শুরু হয়), তাহলে Nest সেটাকে JSON হিসেবে parse করার চেষ্টা করে। তারপর `value` টা সংশ্লিষ্ট handler এ পাস করা হয়।

---

## 9. Outgoing Message

Event publish করার বা message পাঠানোর আগে, Nest outgoing Kafka message গুলো serialize করে। Serialization টা `ClientKafkaProxy` এর `emit()` আর `send()` method এ পাস করা argument গুলোতে, আর একটা `@MessagePattern()` method থেকে return হওয়া value গুলোতে apply হয়। যেসব value string বা buffer না, সেগুলোকে `JSON.stringify()` অথবা `toString()` prototype method দিয়ে "stringify" করা হয়।

```typescript
@Controller()
export class HeroesController {
  @MessagePattern('hero.kill.dragon')
  killDragon(@Payload() message: KillDragonMessage): any {
    const dragonId = message.dragonId;
    const items = [
      { id: 1, name: 'Mythical Sword' },
      { id: 2, name: 'Key to Dungeon' },
    ];
    return items;
  }
}
```

> **Hint:** `@Payload()` টা `@nestjs/microservices` package থেকে import করা হয়।

`key` আর `value` property সহ একটা object পাস করে, outgoing message কে key ও করা যায়। Message key করা [co-partitioning requirement](https://docs.confluent.io/current/ksql/docs/developer-guide/partition-data.html#co-partitioning-requirements) মেটানোর জন্য গুরুত্বপূর্ণ।

```typescript
@Controller()
export class HeroesController {
  @MessagePattern('hero.kill.dragon')
  killDragon(@Payload() message: KillDragonMessage): any {
    const realm = 'Nest';
    const heroId = message.heroId;
    const dragonId = message.dragonId;

    const items = [
      { id: 1, name: 'Mythical Sword' },
      { id: 2, name: 'Key to Dungeon' },
    ];

    return {
      headers: {
        realm,
      },
      key: heroId,
      value: items,
    };
  }
}
```

এই format এ পাস করা message এ `headers` property তে custom header ও রাখা যায়। Header value গুলো অবশ্যই `string` বা `Buffer` type এর হতে হবে।

```typescript
@Controller()
export class HeroesController {
  @MessagePattern('hero.kill.dragon')
  killDragon(@Payload() message: KillDragonMessage): any {
    const realm = 'Nest';
    const heroId = message.heroId;
    const dragonId = message.dragonId;

    const items = [
      { id: 1, name: 'Mythical Sword' },
      { id: 2, name: 'Key to Dungeon' },
    ];

    return {
      headers: {
        kafka_nestRealm: realm,
      },
      key: heroId,
      value: items,
    };
  }
}
```

---

## 10. Event-Based

Service গুলোর মধ্যে message exchange করার জন্য request-response method আদর্শ হলেও, তোমার message style যদি event-based হয় (Kafka এর জন্য যেটা ideal) — অর্থাৎ, তুমি যদি response এর জন্য **অপেক্ষা না করেই** event publish করতে চাও — তাহলে এটা তেমন suitable না। সেক্ষেত্রে, request-response এর জন্য দরকারি দুটো topic maintain করার overhead টা তুমি চাইবে না।

আরো জানতে [Overview: Event-based](https://docs.nestjs.com/microservices/basics#event-based) আর [Overview: Publishing events](https://docs.nestjs.com/microservices/basics#publishing-events) দেখো।

---

## 11. Context

আরো complex scenario তে, incoming request সম্পর্কে অতিরিক্ত তথ্য দরকার হতে পারে। Kafka transporter ব্যবহার করার সময়, `KafkaContext` object access করা যায়।

```typescript
@MessagePattern('hero.kill.dragon')
killDragon(@Payload() message: KillDragonMessage, @Ctx() context: KafkaContext) {
  console.log(`Topic: ${context.getTopic()}`);
}
```

> **Hint:** `@Payload()`, `@Ctx()`, আর `KafkaContext` — এগুলো `@nestjs/microservices` package থেকে import করা হয়।

Original Kafka `IncomingMessage` object access করতে, নিচের মতো `KafkaContext` object এর `getMessage()` method ব্যবহার করো:

```typescript
@MessagePattern('hero.kill.dragon')
killDragon(@Payload() message: KillDragonMessage, @Ctx() context: KafkaContext) {
  const originalMessage = context.getMessage();
  const partition = context.getPartition();
  const { headers, timestamp } = originalMessage;
}
```

`IncomingMessage` object নিচের interface follow করে:

```typescript
interface IncomingMessage {
  topic: string;
  partition: number;
  timestamp: string;
  size: number;
  attributes: number;
  offset: string;
  key: any;
  value: any;
  headers: Record<string, any>;
}
```

তোমার handler যদি প্রতিটা message process করতে অনেক সময় নেয়, তাহলে `heartbeat` callback ব্যবহার করার কথা ভাবতে পারো। `heartbeat` function পেতে, নিচের মতো `KafkaContext` এর `getHeartbeat()` method ব্যবহার করো:

```typescript
@MessagePattern('hero.kill.dragon')
async killDragon(@Payload() message: KillDragonMessage, @Ctx() context: KafkaContext) {
  const heartbeat = context.getHeartbeat();

  // কিছুটা slow processing
  await doWorkPart1();

  // sessionTimeout পার না হওয়ার জন্য heartbeat পাঠাও
  await heartbeat();

  // আবার কিছুটা slow processing
  await doWorkPart2();
}
```

---

## 12. Naming Convention

Nest microservice এর client আর server component এর মধ্যে collision এড়াতে, Kafka microservice component গুলো `client.clientId` আর `consumer.groupId` option এর সাথে তাদের role এর একটা description যোগ করে দেয়। Default ভাবে, `ClientKafkaProxy` component গুলো দুই option এর সাথেই `-client` যোগ করে, আর `ServerKafka` component গুলো `-server` যোগ করে। নিচের comment গুলোতে দেখানো আছে দেওয়া value গুলো কীভাবে transform হয়।

```typescript
const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
  transport: Transport.KAFKA,
  options: {
    client: {
      clientId: 'hero', // hero-server
      brokers: ['localhost:9092'],
    },
    consumer: {
      groupId: 'hero-consumer', // hero-consumer-server
    },
  },
});
```

আর client এর ক্ষেত্রে:

```typescript
@Client({
  transport: Transport.KAFKA,
  options: {
    client: {
      clientId: 'hero', // hero-client
      brokers: ['localhost:9092'],
    },
    consumer: {
      groupId: 'hero-consumer', // hero-consumer-client
    },
  },
})
client: ClientKafkaProxy;
```

> **Hint:** যোগ হওয়া suffix পরিবর্তন করতে, `postfixId` option সেট করো। Client আর consumer এর naming convention আরো customize করতে, তোমার নিজের custom provider এ `ClientKafka` আর `ServerKafka` class extend করে constructor override করো।

Kafka microservice message pattern এ request আর reply channel এর জন্য দুটো topic ব্যবহার হয় বলে, reply pattern টা request topic থেকে derive করা হয়। Default ভাবে, reply topic এর নাম হয় request topic এর নামের শেষে `.reply` যোগ করে।

```typescript
onModuleInit() {
  this.client.subscribeToResponseOf('hero.get'); // hero.get.reply
}
```

> **Hint:** Reply topic এর naming convention customize করতে, তোমার নিজের custom provider এ `ClientKafka` class extend করে `getResponsePatternName()` method override করো।

---

## 13. Retriable Exception

অন্য transporter এর মতোই, unhandled exception গুলো automatically একটা "user-friendly" format এ convert হয়ে যায়। তবে, কিছু edge case এ তুমি এই mechanism বাইপাস করে `kafkajs` driver কেই exception consume করতে দিতে চাইতে পারো। একটা message process করার সময় exception throw করলে, `kafkajs` কে সেটা **retry** (আবার deliver) করতে নির্দেশ দেওয়া হয় — মানে হলো, message (বা event) handler trigger হলেও, offset টা Kafka তে commit হবে না।

> **⚠️ Warning:** Event handler এর জন্য (event-based communication), default ভাবে সব unhandled exception কে **retriable exception** ধরা হয়।

Message handler এর জন্য, dedicated `KafkaRetriableException` class ব্যবহার করো, নিচের মতো:

```typescript
throw new KafkaRetriableException('...');
```

> **Hint:** `KafkaRetriableException` class টা `@nestjs/microservices` package থেকে export করা হয়।

---

## 14. Custom Exception Handling

Default error handling mechanism এর পাশাপাশি, retry logic manage করার জন্য Kafka event এর জন্য একটা custom exception filter বানানো যায়। যেমন, নিচের filter টা একটা configure করা সংখ্যক retry এর পর একটা problematic event skip করে দেয়:

```typescript
import { Catch, ArgumentsHost, ExceptionFilter, Logger } from '@nestjs/common';
import { KafkaContext } from '@nestjs/microservices';
import { Producer } from 'kafkajs';

@Catch()
export class KafkaMaxRetryExceptionFilter implements ExceptionFilter {
  private readonly logger = new Logger(KafkaMaxRetryExceptionFilter.name);

  constructor(
    private readonly producer: Producer,
    private readonly maxRetries: number,
    // Optional custom function, max retries পার হয়ে গেলে execute হয়
    private readonly skipHandler?: (message: any) => Promise<void>,
  ) {}

  async catch(exception: unknown, host: ArgumentsHost) {
    const kafkaContext = host.switchToRpc().getContext<KafkaContext>();
    const message = kafkaContext.getMessage();
    const currentRetryCount = this.getRetryCountFromContext(kafkaContext);

    if (currentRetryCount >= this.maxRetries) {
      this.logger.warn(
        `Max retries (${
          this.maxRetries
        }) exceeded for message: ${JSON.stringify(message)}`,
      );

      if (this.skipHandler) {
        try {
          await this.skipHandler(message);
        } catch (err) {
          this.logger.error('Error in skipHandler:', err);
        }
      }

      try {
        await this.commitOffset(kafkaContext);
      } catch (commitError) {
        this.logger.error('Failed to commit offset:', commitError);
      }
      return; // Exception propagate করা বন্ধ করো
    }

    // Retry count বাড়িয়ে একই topic এ message আবার publish করো
    try {
      await this.republishWithRetry(kafkaContext, currentRetryCount + 1);
      await this.commitOffset(kafkaContext);
    } catch (republishError) {
      this.logger.error(
        'Failed to republish message for retry:',
        republishError,
      );
      // KafkaJS যাতে message টা retry করে, তাই আবার throw করো
      throw exception;
    }
  }

  private getRetryCountFromContext(context: KafkaContext): number {
    const headers = context.getMessage().headers || {};
    const retryHeader = headers['retry-count'];
    if (!retryHeader) {
      return 0;
    }
    // Header value গুলো Buffer, তাই আগে string এ convert করো
    const value = Buffer.isBuffer(retryHeader)
      ? retryHeader.toString()
      : String(retryHeader);
    return parseInt(value, 10) || 0;
  }

  private async republishWithRetry(
    context: KafkaContext,
    retryCount: number,
  ): Promise<void> {
    const topic = context.getTopic();
    const message = context.getMessage();

    await this.producer.send({
      topic,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            'retry-count': retryCount.toString(),
          },
        },
      ],
    });
  }

  private async commitOffset(context: KafkaContext): Promise<void> {
    const consumer = context.getConsumer();
    if (!consumer) {
      throw new Error('Consumer instance is not available from KafkaContext.');
    }

    const topic = context.getTopic();
    const partition = context.getPartition();
    const message = context.getMessage();
    const offset = message.offset;

    if (!topic || partition === undefined || offset === undefined) {
      throw new Error(
        'Incomplete Kafka message context for committing offset.',
      );
    }

    await consumer.commitOffsets([
      {
        topic,
        partition,
        // Offset commit করার সময়, পরের number টা commit করো (অর্থাৎ, current offset + 1)
        offset: (Number(offset) + 1).toString(),
      },
    ]);
  }
}
```

এই filter টা একটা Kafka event process করার চেষ্টা একটা configure করা সংখ্যক বার পর্যন্ত retry করে। কোনো exception ঘটলে, এটা একটা বাড়ানো `retry-count` header সহ message টা একই topic এ আবার publish করে, তারপর current offset commit করে দেয়। সর্বোচ্চ retry সংখ্যায় পৌঁছে গেলে, এটা custom `skipHandler` (যদি দেওয়া থাকে) call করে আর offset commit করে দেয়, problematic event টা skip করে। এর ফলে পরবর্তী event গুলো কোনো বাধা ছাড়াই process হতে পারে।

এই filter টা global ভাবে বা controller level এ register করা যায়। এর জন্য একটা Kafka producer instance দরকার:

```typescript
import { Inject, Injectable } from '@nestjs/common';
import { Producer } from 'kafkajs';

@Injectable()
export class AppKafkaRetryFilter extends KafkaMaxRetryExceptionFilter {
  constructor(@Inject('KAFKA_PRODUCER') producer: Producer) {
    super(producer, 5); // maxRetries = 5
  }
}
```

```typescript
@Controller()
@UseFilters(AppKafkaRetryFilter)
export class MyEventHandler {
  @EventPattern('your-topic')
  async handleEvent(@Payload() data: any, @Ctx() context: KafkaContext) {
    // তোমার event processing logic...
  }
}
```

তোমার module এ Kafka producer টা provide করতে ভুলো না:

```typescript
import { Kafka } from 'kafkajs';

@Module({
  providers: [
    AppKafkaRetryFilter,
    {
      provide: 'KAFKA_PRODUCER',
      useFactory: async () => {
        const kafka = new Kafka({ brokers: ['localhost:9092'] });
        const producer = kafka.producer();
        await producer.connect();
        return producer;
      },
    },
  ],
})
export class AppModule {}
```

---

## 15. Offset Commit করা

Kafka নিয়ে কাজ করার সময় offset commit করাটা অপরিহার্য। Default ভাবে, message গুলো একটা নির্দিষ্ট সময় পর automatically commit হয়ে যায় (দেখো [KafkaJS autocommit documentation](https://kafka.js.org/docs/consuming#autocommit))। `KafkaContext` তোমাকে manually offset commit করার জন্য active consumer access দেয়। এই consumer টা হলো KafkaJS consumer, আর এটা [native KafkaJS implementation](https://kafka.js.org/docs/consuming#manual-committing) এর মতোই কাজ করে।

```typescript
@EventPattern('user.created')
async handleUserCreated(@Payload() data: IncomingMessage, @Ctx() context: KafkaContext) {
  // business logic

  const { offset } = context.getMessage();
  const partition = context.getPartition();
  const topic = context.getTopic();
  const consumer = context.getConsumer();
  // পরের consume করার offset commit করো (অর্থাৎ, current offset + 1)
  await consumer.commitOffsets([
    { topic, partition, offset: (Number(offset) + 1).toString() },
  ]);
}
```

Auto-committing disable করতে, `run` configuration এ `autoCommit: false` সেট করো, নিচের মতো:

```typescript
const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
  transport: Transport.KAFKA,
  options: {
    client: {
      brokers: ['localhost:9092'],
    },
    run: {
      autoCommit: false,
    },
  },
});
```

---

## 16. Instance Status Update

Connection এবং underlying driver instance এর state সম্পর্কে real-time update পেতে, `status` stream এ subscribe করো। এই stream বেছে নেওয়া driver অনুযায়ী নির্দিষ্ট status update দেয়। Kafka driver এর ক্ষেত্রে, `status` stream `connected`, `disconnected`, `rebalancing`, `crashed`, আর `stopped` — এই পাঁচ ধরনের event emit করে।

```typescript
this.client.status.subscribe((status: KafkaStatus) => {
  console.log(status);
});
```

> **Hint:** `KafkaStatus` type টা `@nestjs/microservices` package থেকে import করা হয়।

একইভাবে, server এর status সম্পর্কে notification পেতে, server এর `status` stream এও subscribe করা যায়:

```typescript
const server = app.connectMicroservice<MicroserviceOptions>(...);
server.status.subscribe((status: KafkaStatus) => {
  console.log(status);
});
```

---

## 17. Underlying Producer এবং Consumer

আরো advanced use case এ, underlying producer আর consumer instance access করার দরকার হতে পারে — যেমন, manually connection close করা বা driver-specific method ব্যবহার করা। তবে, বেশিরভাগ ক্ষেত্রে driver directly access করার **দরকার হয় না**।

এর জন্য, `ClientKafkaProxy` instance এর expose করা `producer` আর `consumer` property ব্যবহার করো।

```typescript
const producer = this.client.producer;
const consumer = this.client.consumer;
```
