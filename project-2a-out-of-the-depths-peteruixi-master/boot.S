.section .multiboot_hdr
.set MULTIBOOT_MAGIC, 0xe85250d6
.set ARCH, 0x0

multiboot_hdr:
	.long MULTIBOOT_MAGIC
	.long ARCH
	.long hdr_end - multiboot_hdr
	.long -(MULTIBOOT_MAGIC + (hdr_end - multiboot_hdr))

	/* no special tags */
	.word 0, 0
	.long 8

hdr_end:

stackBot:

.skip 512

stackTop:
	
.section .text
.code32
	
.globl kernel_entry
.type kernel_entry, @function
	
kernel_entry:
	mov $stackTop, %esp
	call  KERNEL_MAIN
	cli
	
	

end:
	hlt
	jmp end
