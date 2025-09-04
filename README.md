# Jeihyuck-rolling-k-auto-trade-KIS-refacored



flowchart TD
    A[사용자 질의] --> B[Intent Classifier<br/>(purchase / cs / hybrid)]
    B -->|purchase| C[Router → Table RAG (가격/재고/프로모션/배송)]
    B -->|cs| D[Router → Doc RAG / KG (설치/보증/오류코드)]
    B -->|hybrid| E[[Parallel Fan-out]]
    E --> C
    E --> D
    C --> F[Result Normalizer<br/>(단위/용어 정규화)]
    D --> F
    F --> G[Answer Composer<br/>(병합·충돌해결·근거표기)]
    G --> H[Guardrails<br/>(SLA·타임아웃·폴백)]
    H --> I[Response Formatter<br/>(표/리스트/비교표/CTA)]
    I --> J[사용자 응답 + 출처/기준시각]
