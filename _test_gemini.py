import os
os.environ['GOOGLE_API_KEY'] = os.environ.get('GOOGLE_API_KEY','')
from utils.ai import chat_anything, ai_provider
print('Provider:', ai_provider())
ans = chat_anything([{'role':'user','content':'Reply with: Hello from Gemini'}])
print(ans)
