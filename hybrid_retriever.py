"""
Hybrid Search Retriever (BM25 + Vector) for DataLens Golden Queries.

This module implements a hybrid retrieval strategy combining:
- BM25: Keyword-based lexical search
- Vector: Semantic similarity search via ChromaDB
- Fusion: Reciprocal Rank Fusion (RRF) for result ranking

Benefits:
- BM25 catches exact SQL syntax matches
- Vector catches semantic intent matches
- Combined: Best of both worlds with minimal latency overhead
"""

import os
from langchain_community.retrievers import BM25Retriever
from langchain_chroma import Chroma
from langchain_openai import AzureOpenAIEmbeddings
from langchain_core.documents import Document


class HybridRetriever:
    """
    Hybrid retriever combining BM25 and vector search with Reciprocal Rank Fusion.
    """

    def __init__(self, bm25_retriever, vector_retriever, weights=(0.4, 0.6)):
        self.bm25_retriever = bm25_retriever
        self.vector_retriever = vector_retriever
        self.bm25_weight, self.vector_weight = weights

    def invoke(self, query: str, k: int = 4) -> list[Document]:
        """
        Retrieve documents using hybrid search with RRF fusion.

        Args:
            query: Search query
            k: Number of results to return

        Returns:
            List of Document objects ranked by RRF score
        """
        # Get results from both retrievers
        bm25_docs = self.bm25_retriever.invoke(query)
        vector_docs = self.vector_retriever.invoke(query)

        # Build RRF ranking (Reciprocal Rank Fusion)
        # Score = sum(1 / (rank + 1)) for each retriever weighted by importance
        rrf_scores = {}

        for rank, doc in enumerate(bm25_docs):
            content = doc.page_content
            rrf_scores[content] = rrf_scores.get(content, 0) + (
                self.bm25_weight / (rank + 1)
            )

        for rank, doc in enumerate(vector_docs):
            content = doc.page_content
            rrf_scores[content] = rrf_scores.get(content, 0) + (
                self.vector_weight / (rank + 1)
            )

        # Sort by RRF score and return top k
        sorted_results = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)[
            :k
        ]
        return [Document(page_content=content) for content, _ in sorted_results]


def get_hybrid_retriever(golden_queries: list[str]) -> HybridRetriever:
    """
    Create a hybrid BM25 + vector retriever for golden SQL queries.

    Args:
        golden_queries: List of verified SQL query strings

    Returns:
        HybridRetriever combining BM25 and vector search
    """

    # 1. Initialize embeddings for vector search
    embeddings = AzureOpenAIEmbeddings(
        azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME_EMBEDDINGS"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version=os.getenv("AZURE_API_VERSION"),
    )

    # 2. Create BM25 retriever from query texts (no external DB needed)
    bm25_retriever = BM25Retriever.from_texts(golden_queries, k=2)

    # 3. Create vector retriever from existing ChromaDB
    vectorstore = Chroma(persist_directory="./chroma_db", embedding_function=embeddings)
    vector_retriever = vectorstore.as_retriever(search_kwargs={"k": 2})

    # 4. Combine with Reciprocal Rank Fusion (RRF)
    # Weights: 40% BM25 (keyword), 60% vector (semantic)
    hybrid_retriever = HybridRetriever(
        bm25_retriever, vector_retriever, weights=(0.4, 0.6)
    )

    return hybrid_retriever
