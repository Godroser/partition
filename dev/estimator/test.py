import string

line = "  └─HashAgg_31"

index = line.find("└─") if "└─" in line else line.find("├─")
print(index)