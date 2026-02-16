import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'PrkDB',
  description: 'High-performance event streaming database written in Rust',
  
  // Set base URL for GitHub Pages subdirectory deployment
  base: '/prkdb/',
  
  themeConfig: {
    nav: [
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'GitHub', link: 'https://github.com/prk-Jr/prkdb' }
    ],
    
    sidebar: {
      '/guide/': [
        {
          text: 'Introduction',
          items: [
            { text: 'Getting Started', link: '/guide/getting-started' },
            { text: 'Roadmap', link: '/guide/roadmap' }
          ]
        },
        {
          text: 'Core Concepts',
          items: [
            { text: 'Replication', link: '/guide/replication' },
            { text: 'Custom Adapters', link: '/guide/custom-adapter' },
            { text: 'ORM Dialects', link: '/guide/orm-dialects-quickstart' }
          ]
        },
        {
          text: 'Advanced',
          items: [
            { text: 'Deployment', link: '/guide/deployment' },
            { text: 'Streaming & Kafka', link: '/guide/streaming-kafka-comparison' },
            { text: 'Metrics', link: '/guide/metrics' }
          ]
        }
      ]
    }
  }
})
