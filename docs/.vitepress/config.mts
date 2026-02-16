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
