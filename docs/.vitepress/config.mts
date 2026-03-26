import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'PrkDB',
  description:
    'A persistent, distributed key-value store with advanced features',
  base: '/prkdb/',
  ignoreDeadLinks: true,
  themeConfig: {
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'Reference', link: '/guide/metrics' },
    ],

    sidebar: [
      {
        text: 'Introduction',
        items: [
          { text: 'Getting Started', link: '/guide/getting-started' },
          { text: 'Roadmap', link: '/guide/roadmap' },
          { text: 'Deployment', link: '/guide/deployment' },
        ],
      },
      {
        text: 'Core Concepts',
        items: [
          { text: 'Replication', link: '/guide/replication' },
          { text: 'Custom Adapters', link: '/guide/custom-adapter' },
          { text: 'ORM Dialects', link: '/guide/orm-dialects-quickstart' },
        ],
      },
      {
        text: 'Architecture',
        items: [
          { text: 'Partitions & Sharding', link: '/guide/architecture/partitions' },
          { text: 'Leader Election', link: '/guide/architecture/leader-election' },
        ],
      },
      {
        text: 'Features',
        items: [
          { text: 'Transactions', link: '/guide/features/transactions' },
          { text: 'Time-To-Live (TTL)', link: '/guide/features/ttl' },
          { text: 'Secondary Indexes', link: '/guide/features/secondary-indexes' },
        ],
      },
      {
        text: 'Schema & SDK',
        items: [
          { text: 'Schema Registry', link: '/guide/schema-registry' },
          { text: 'Cross-Language Codegen', link: '/guide/codegen' },
        ],
      },
      {
        text: 'Client Integration',
        items: [
          { text: 'Smart Client', link: '/guide/client/smart-client' },
        ],
      },
      {
        text: 'Advanced',
        items: [
          {
            text: 'Streaming & Kafka',
            link: '/guide/streaming-kafka-comparison',
          },
          { text: 'Metrics', link: '/guide/metrics' },
        ],
      },
    ],

    socialLinks: [{ icon: 'github', link: 'https://github.com/prk-Jr/prkdb' }],
  },
})
