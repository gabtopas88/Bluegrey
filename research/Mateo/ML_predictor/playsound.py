import subprocess
def playsound():
    subprocess.run(["afplay", f"/System/Library/Sounds/{'Glass'}.aiff"])

if __name__ == "__main__":
    playsound()