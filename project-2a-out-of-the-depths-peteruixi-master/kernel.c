#define VGA_ADDRESS  0xB8000
#define WHITE_COLOR  15
typedef unsigned short UINT16;
UINT16* TERMINAL_BUFFER;

//extern void print_ok();
static UINT16 VGA_DefaultEntry(unsigned char to_print){
  return (UINT16) to_print | (UINT16)WHITE_COLOR << 8;
}
void KERNEL_MAIN(){
  TERMINAL_BUFFER = (UINT16*) VGA_ADDRESS;
  TERMINAL_BUFFER[0] = VGA_DefaultEntry('O');
  TERMINAL_BUFFER[1] = VGA_DefaultEntry('K');

}
