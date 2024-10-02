
def prompt_user_to_continue(message: str)->bool:
    
    while True:
        
        choice = input(f"{message},\n do you want to continue? y/n\n")
        
        if choice.lower() == 'y':
            return True
        elif choice.lower() == 'n':
            return False
        else:
            print("Invalid input. Please enter 'y' or 'n'.\n")
    
    