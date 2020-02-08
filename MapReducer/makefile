all: analogh analogp analogh2

analogh: analogh.o
	gcc -ansi -o analogh analogh.o -pthread
analogh.o: analogh.c Buf.h MapParameter.h structlog.h
	gcc -c analogh.c
analogh2: analogh2.o
	gcc -ansi -o analogh2 analogh2.o -pthread
analogh2.o: analogh2.c Buf.h MapParameter.h structlog.h
	gcc -c analogh2.c
analogp: analogp.o
	gcc -ansi -o analogp analogp.o
analogp.o: analogp.c structlog.h
	gcc -c analogp.c
clean:
	rm *.o analogh analogp analogh2
