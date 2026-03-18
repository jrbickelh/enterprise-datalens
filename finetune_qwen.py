"""
QLoRA Fine-Tuning for Qwen 2.5-Coder 7B

Trains Qwen to generate SQL from natural language queries.
Uses QLoRA (4-bit quantization + LoRA adapters) for efficient fine-tuning.

Requirements:
  pip install transformers peft bitsandbytes datasets torch
  GPU with 8GB+ VRAM (RTX 3060, RTX 4060, A10, etc.)

Usage:
  uv run python finetune_qwen.py

Output:
  - saves/ → Fine-tuned LoRA adapters
  - events/ → TensorBoard logs
"""

import json
from pathlib import Path

# Check if dependencies are available
try:
    import torch
    from transformers import (
        AutoModelForCausalLM,
        AutoTokenizer,
        BitsAndBytesConfig,
        TrainingArguments,
        Trainer,
    )
    from peft import LoraConfig, get_peft_model
    from datasets import Dataset

    HAS_TRANSFORMERS = True
except ImportError:
    HAS_TRANSFORMERS = False
    print("⚠️  Dependencies not installed. Run:")
    print("   pip install transformers peft bitsandbytes datasets torch")
    print("   (or: uv pip install ...)")


def load_training_data(dataset_file="finetune_dataset.jsonl"):
    """Load fine-tuning dataset from JSONL."""
    if not Path(dataset_file).exists():
        print(f"❌ Dataset file not found: {dataset_file}")
        print("   First run: uv run python finetune_data_prep.py")
        return None

    examples = []
    with open(dataset_file, "r") as f:
        for line in f:
            examples.append(json.loads(line))

    print(f"✅ Loaded {len(examples)} training examples")
    return examples


def create_prompt_template(example):
    """Convert example to instruction-following format."""
    return f"""You are a SQL expert. Generate SQL queries from natural language questions.

Instruction: {example['instruction']}
Input: {example['input']}

Output (SQL query):
{example['output']}"""


def prepare_dataset(examples, tokenizer, max_length=512):
    """Convert examples to tokenized dataset."""

    prompts = [create_prompt_template(ex) for ex in examples]

    # Tokenize
    encodings = tokenizer(
        prompts,
        max_length=max_length,
        truncation=True,
        padding="max_length",
        return_tensors="pt",
    )

    encodings["labels"] = encodings["input_ids"].clone()

    dataset = Dataset.from_dict(
        {
            "input_ids": encodings["input_ids"],
            "attention_mask": encodings["attention_mask"],
            "labels": encodings["labels"],
        }
    )

    return dataset


def setup_quantization():
    """Configure 4-bit quantization for QLoRA."""
    quantization_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_compute_dtype=torch.float16,
        bnb_4bit_use_double_quant=True,
        bnb_4bit_quant_type="nf4",
    )
    return quantization_config


def setup_lora():
    """Configure LoRA adapters."""
    lora_config = LoraConfig(
        r=8,  # LoRA rank
        lora_alpha=16,
        target_modules=["q_proj", "v_proj"],  # Qwen attention layers
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM",
    )
    return lora_config


def train(
    model_name="Qwen/Qwen2.5-Coder-7B",
    dataset_file="finetune_dataset.jsonl",
    output_dir="saves/qwen-7b-sql",
    num_epochs=3,
    batch_size=4,
    learning_rate=2e-4,
):
    """
    Fine-tune Qwen 7B with QLoRA.

    Args:
        model_name: HuggingFace model ID
        dataset_file: Path to JSONL training data
        output_dir: Where to save fine-tuned adapters
        num_epochs: Training epochs
        batch_size: Batch size (4-8 for 8GB GPU)
        learning_rate: Learning rate (2e-4 is standard for QLoRA)
    """

    if not HAS_TRANSFORMERS:
        print("❌ Please install transformers, peft, bitsandbytes first")
        return

    print("🚀 Starting QLoRA Fine-Tuning")
    print(f"   Model: {model_name}")
    print(f"   Epochs: {num_epochs}")
    print(f"   Batch size: {batch_size}")
    print(f"   Learning rate: {learning_rate}")

    # 1. Load model with 4-bit quantization
    print("\n📦 Loading model (4-bit quantized)...")
    quantization_config = setup_quantization()

    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        quantization_config=quantization_config,
        device_map="auto",
        trust_remote_code=True,
    )

    # 2. Load tokenizer
    print("📖 Loading tokenizer...")
    tokenizer = AutoTokenizer.from_pretrained(
        model_name,
        trust_remote_code=True,
        padding_side="right",
    )
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    # 3. Load training data
    print("\n📚 Preparing training data...")
    examples = load_training_data(dataset_file)
    if examples is None:
        return

    dataset = prepare_dataset(examples, tokenizer)
    print(f"   Tokenized {len(dataset)} examples")

    # 4. Setup LoRA
    print("\n⚙️  Setting up LoRA adapters...")
    lora_config = setup_lora()
    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()

    # 5. Setup training arguments
    print("\n🎓 Setting up training...")
    training_args = TrainingArguments(
        output_dir=output_dir,
        num_train_epochs=num_epochs,
        per_device_train_batch_size=batch_size,
        gradient_accumulation_steps=2,
        learning_rate=learning_rate,
        weight_decay=0.01,
        warmup_steps=100,
        save_steps=50,
        logging_steps=10,
        save_total_limit=3,
        report_to="tensorboard",
        logging_dir="events/",
        optim="paged_adamw_8bit",
    )

    # 6. Create trainer and train
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=dataset,
        data_collator=None,  # Use default
    )

    print("\n🔥 Starting training (this will take 30-60 minutes on GPU)...")
    trainer.train()

    # 7. Save final model
    print("\n💾 Saving fine-tuned model...")
    model.save_pretrained(output_dir)
    tokenizer.save_pretrained(output_dir)

    print("\n✅ Fine-tuning complete!")
    print(f"   Saved to: {output_dir}")
    print("\n📊 Next steps:")
    print("   1. Test the model: python inference_qwen.py")
    print("   2. Deploy: Use vLLM or Ollama for inference")
    print("   3. Integrate: Update agent_graph.py to use local model")


if __name__ == "__main__":
    # Step 1: Generate dataset if needed
    dataset_file = "finetune_dataset.jsonl"
    if not Path(dataset_file).exists():
        print("📊 Generating training dataset...")
        from finetune_data_prep import save_finetuning_dataset

        save_finetuning_dataset(dataset_file)

    # Step 2: Start training
    if torch.cuda.is_available():
        print(f"✅ GPU available: {torch.cuda.get_device_name(0)}")
        print(f"   Memory: {torch.cuda.get_device_properties(0).total_memory / 1e9:.1f} GB")
        train()
    else:
        print("❌ No GPU detected. QLoRA requires CUDA.")
        print("   Options:")
        print("   1. Run on GPU instance (AWS, GCP, Azure)")
        print("   2. Use Google Colab (free GPU)")
        print("   3. Use local GPU (RTX 3060, RTX 4060, M1/M2 Mac, etc.)")
