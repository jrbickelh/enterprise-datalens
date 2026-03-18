#!/usr/bin/env python3
"""
CLI Tool: Query DataLens Agent from the Command Line

Usage:
  uv run python cli.py --query "Show Q4 revenue by region"
  uv run python cli.py --query "Detect anomalies in transaction amounts" --verbose
  uv run python cli.py --query "Forecast next 3 months of revenue" --hitl
"""

import argparse
import json
import sys
import time
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


def run_agent_query(
    query: str, verbose: bool = False, hitl: bool = False, config: Optional[dict] = None
) -> dict:
    """
    Execute a query through the DataLens agent graph.

    Args:
        query: User's natural language query
        verbose: Print intermediate reasoning steps
        hitl: Use Human-in-the-Loop (safe mode) instead of autonomous
        config: Optional configuration overrides

    Returns:
        dict with 'response', 'messages', 'cost', 'latency'
    """
    from agent_service import run_agent

    start_time = time.time()

    try:
        result = run_agent(query, hitl=hitl, verbose=verbose)
        latency = time.time() - start_time

        if verbose:
            print(f"\n⏱️  Latency: {latency:.2f}s")

        return {
            "status": "success",
            "response": result.get("response", ""),
            "messages": result.get("messages", []),
            "cost": result.get("cost", 0.0),
            "latency_seconds": latency,
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "latency_seconds": time.time() - start_time,
        }


def format_output(result: dict, verbose: bool = False) -> str:
    """Format result for CLI output."""
    output = []

    if result["status"] == "error":
        output.append(f"❌ Error: {result['error']}")
        return "\n".join(output)

    # Response
    output.append(f"✅ Response:\n{result['response']}\n")

    # Cost & Latency
    output.append(f"💰 Cost: ${result['cost']:.4f}")
    output.append(f"⏱️  Latency: {result['latency_seconds']:.2f}s")

    if verbose:
        output.append(f"\n📊 Messages Exchanged: {len(result['messages'])}")
        for i, msg in enumerate(result["messages"][-3:], 1):  # Show last 3
            output.append(f"  [{i}] {msg[:100]}...")

    return "\n".join(output)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Query DataLens AI Agent from the command line",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python cli.py --query "Show Q4 revenue by region"
  uv run python cli.py --query "Detect anomalies" --verbose
  uv run python cli.py --query "Forecast revenue" --hitl
        """,
    )

    parser.add_argument(
        "--query", "-q", type=str, required=True, help="Natural language query for the agent"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show intermediate reasoning and message count",
    )
    parser.add_argument(
        "--hitl",
        action="store_true",
        help="Enable Human-in-the-Loop (safe mode) for expensive operations",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )

    args = parser.parse_args()

    # Run the query
    result = run_agent_query(args.query, verbose=args.verbose, hitl=args.hitl)

    # Format and output
    if args.output == "json":
        print(json.dumps(result, indent=2))
    else:
        print(format_output(result, verbose=args.verbose))

    # Exit code
    sys.exit(0 if result["status"] == "success" else 1)


if __name__ == "__main__":
    main()
