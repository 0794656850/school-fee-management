import importlib, sys
m = importlib.import_module('app')
print('Imported app, has app:', hasattr(m,'app'))