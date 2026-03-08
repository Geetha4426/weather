import os

folder = 'c:/Users/acer/.gemini/antigravity/scratch/5min_trade/weather_prediction/ml'
for f in os.listdir(folder):
    if not f.endswith('.py'): continue
    path = os.path.join(folder, f)
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    
    out = []
    i = 0
    while i < len(lines):
        if lines[i].startswith('<<<<<<< HEAD'):
            i += 1
            continue
        if lines[i].startswith('======='):
            # Skip until >>>>>>>
            while i < len(lines) and not lines[i].startswith('>>>>>>>'):
                i += 1
            if i < len(lines):
                i += 1 # skip >>>>>>>
            continue
        out.append(lines[i])
        i += 1
        
    with open(path, 'w', encoding='utf-8') as file:
        file.writelines(out)

print('Cleaned conflict markers')
