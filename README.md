# Lion Insurance Automated COI Handling Bot

## How it works

```mermaid
flowchart LR;
    A[Gmail inbox change] -->|Pub/Sub push| B[Cloud Run]
    B -->|Infer holder details| C[OpenAI API]
    C -->|Notify user| D[Telegram Bot]
    D -->|User approval| B
    B --> E[Send COI]
    
    style E fill:#90EE90,stroke:#228B22,stroke-width:4px
```
