from src.agent import ask

question = "How many trips were there?"

print(f"Q: {question}\n")
answer = ask(question)
print(f"\n{'='*60}")
print(f"ANSWER: {answer}")