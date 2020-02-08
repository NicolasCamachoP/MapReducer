/*
Autores:
Nicolas Camacho
Jhonnier Coronado
Brayan Gonzales
Objetivo: Crearr un programa que mediante hilos y semaforos analice un archivo de logs
*/

#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <stdbool.h>
#include <time.h>
#include <sys/time.h>
#include "structlog.h"
#include "MapParameter.h"
#include "Buf.h"
#include <pthread.h>

#include <signal.h>
#include <semaphore.h>

#define MAXLIN 150

/*
Creación de las variables globales
logs: Guarda la información del archivo de logs
buffers: Contiene la información que satisface la consulta realizada por el usuario
output: Resultado de la consulta
tam_buffer: Variable que indica cuantos buffers se crearon
Semaforos varios: Semaforos que usan para el productor/consumidor en distintas partes del codigo
*/
Log *logs;
Buf *buffers;
int *output;
int tam_buffer;
sem_t *prod, *cons, *mutex, *master1, *master2, *Redprod, *Redcons, *Redmutex;

/*
Función: signalHandler.
Precondición: El identificador del hilo.
Postcondición: El hilo ha sido finalizado.
Sintesis: Realiza la finalicación del uso del hilo.
*/
void signalHandler(int signum)
{
    pthread_exit(NULL);
}

/*
Función: matarHilos.
Precondición: Los arreglos de hilos correspondientes a los 'mappers' y a los 'reducers', junto con el total de 'mappers' y 'reducers' creados.
Postcondición: Libera las variables globales y finaliza los hilos.
Sintesis: Finalización del programa.
*/
void matarHilos(pthread_t *mappers, pthread_t *reducers, int nmappers, int nreducers)
{
    int i;

    for (i = 0; i < nmappers; i++)
    {
        printf("Mapper con ID %d termina\n", i);
        pthread_kill(mappers[i], SIGUSR1);
    }
    for (i = 0; i < nmappers; i++)
    {
        pthread_join(mappers[i], NULL);
    }
    for (i = 0; i < nreducers; i++)
    {
        printf("Reducer con ID %d termina\n", i);
        pthread_kill(reducers[i], SIGUSR1);
    }
    for (i = 0; i < nreducers; i++)
    {
        pthread_join(reducers[i], NULL);
    }
    free(logs);
    free(buffers);
    free(prod);
    free(cons);
    free(mutex);
    free(master1);
    free(master2);
    free(Redprod);
    free(Redcons);
    free(Redmutex);
}

/*
Función: iniciarSemaforos.
Precondición: El número de reducers.
Postcondición: Asigna memoria a los distintos arreglos de semaforos e inicializa los semáforos.
Sintesis: Prepara los semáforos para el uso en el programa.
*/
void iniciarSemaforos(int nreducers)
{
    int i;
    prod = malloc(tam_buffer * sizeof(sem_t));
    cons = malloc(tam_buffer * sizeof(sem_t));
    mutex = malloc(tam_buffer * sizeof(sem_t));
    master1 = malloc(tam_buffer * sizeof(sem_t));
    master2 = malloc(tam_buffer * sizeof(sem_t));
    Redprod = malloc(nreducers * sizeof(sem_t));
    Redcons = malloc(nreducers * sizeof(sem_t));
    Redmutex = malloc(nreducers * sizeof(sem_t));
    for (i = 0; i < tam_buffer; i++)
    {
        sem_t temp;
        sem_init(&temp, 0, 1);
        prod[i] = temp;

        sem_init(&temp, 0, 0);
        cons[i] = temp;

        sem_init(&temp, 0, 1);
        mutex[i] = temp;

        sem_init(&temp, 0, 0);
        master1[i] = temp;

        sem_init(&temp, 0, 0);
        master2[i] = temp;
    }
    for (i = 0; i < nreducers; i++)
    {
        sem_t temp;
        sem_init(&temp, 0, 1);
        Redprod[i] = temp;

        sem_init(&temp, 0, 0);
        Redcons[i] = temp;

        sem_init(&temp, 0, 1);
        Redmutex[i] = temp;
    }
};

/*
Función: mgetline.
Autora: Ing. Mariela Curiel.
Precondición: Cadena de caracteres en la cual se guardara una linea del archivo que a su vez recibe. También, recibe el tamaño maximo
posible de la línea.
Postcondición: Retorna el tamaño de la linea leida.
Sintesis: Lee el archivo y guarda en la cadena cada linea del archivo.
*/
int mgetline(char *line, int max, FILE *f)
{
    if (fgets(line, max, f) == NULL)
        return (0);
    else
        return (strlen(line));
}

/*
Función: leerarchivo.
Precondición: el nombre del archivo, el número de líneas que se deben leer y el arreglo de logs en el cual se guardará la información de la línea.
Postcondición: El archivo ha sido leído y su información se guardo en logs.
Sintesis: Lee y guarda la información del archivo.
*/
void leerarchivo(const char *logfile, int nlineas, Log *logs)
{
    FILE *archivo;
    char lineas[MAXLIN];
    int i = 0;
    archivo = fopen(logfile, "r");
    if (archivo == NULL)
    {
        printf("Archivo null....");
        exit(1);
    }
    while (mgetline(lineas, sizeof(lineas), archivo) > 0)
    {
        sscanf(lineas, "%i %i %i %i %i %i %i %i %i %i %i %i %i %i %i %i %i %i", &logs[i].datosLog[0], &logs[i].datosLog[1], &logs[i].datosLog[2], &logs[i].datosLog[3], &logs[i].datosLog[4], &logs[i].datosLog[5], &logs[i].datosLog[6], &logs[i].datosLog[7], &logs[i].datosLog[8], &logs[i].datosLog[9], &logs[i].datosLog[10], &logs[i].datosLog[11], &logs[i].datosLog[12], &logs[i].datosLog[13], &logs[i].datosLog[14], &logs[i].datosLog[15], &logs[i].datosLog[16], &logs[i].datosLog[17]);
        i++;
    }

    fclose(archivo);
}

/*
Función: Map
Precondición: Estructura que posee las condiciones de la consulta.
Postcondición: Llena los buffers y realiza productor/consumidor con los 'reducers'.
Sintesis: Asigna a los buffers aquellos logs que cumplan con la consulta.
*/
void Map(mparameter *parametro)
{
    static sigset_t mask;

    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR1);

    if (pthread_sigmask(SIG_BLOCK, &mask, NULL) != 0)
    {
        perror("pthread_sigmask");
        exit(1);
    }
    while (true)
    {
        if (pthread_sigmask(SIG_UNBLOCK, &mask, NULL) != 0)
        {
            perror("pthread_sigmask");
            exit(1);
        }
        sem_wait(&master1[parametro->nBuf]);
        int cont = 0;
        int i = 0;
        Buf bufferL;
        bufferL.key = malloc(sizeof(int) * (parametro->hasta - parametro->desde));
        bufferL.value = malloc(sizeof(int) * (parametro->hasta - parametro->desde));
        bufferL.tam = 0;
        for (i = parametro->desde; i < parametro->hasta; i++)
        {
            if (parametro->signo == 0)
            {
                if (logs[i].datosLog[parametro->columna - 1] < parametro->valor)
                {
                    bufferL.key[bufferL.tam] = logs[i].datosLog[0];
                    bufferL.value[bufferL.tam] = logs[i].datosLog[parametro->columna - 1];
                    bufferL.tam++;
                }
            }
            if (parametro->signo == 1)
            {
                if (logs[i].datosLog[parametro->columna - 1] <= parametro->valor)
                {
                    bufferL.key[bufferL.tam] = logs[i].datosLog[0];
                    bufferL.value[bufferL.tam] = logs[i].datosLog[parametro->columna - 1];
                    bufferL.tam++;
                }
            }
            if (parametro->signo == 2)
            {
                if (logs[i].datosLog[parametro->columna - 1] > parametro->valor)
                {
                    bufferL.key[bufferL.tam] = logs[i].datosLog[0];
                    bufferL.value[bufferL.tam] = logs[i].datosLog[parametro->columna - 1];
                    bufferL.tam++;
                }
            }
            if (parametro->signo == 3)
            {
                if (logs[i].datosLog[parametro->columna - 1] >= parametro->valor)
                {
                    bufferL.key[bufferL.tam] = logs[i].datosLog[0];
                    bufferL.value[bufferL.tam] = logs[i].datosLog[parametro->columna - 1];
                    bufferL.tam++;
                }
            }
            if (parametro->signo == 4)
            {
                if (logs[i].datosLog[parametro->columna - 1] == parametro->valor)
                {
                    bufferL.key[bufferL.tam] = logs[i].datosLog[0];
                    bufferL.value[bufferL.tam] = logs[i].datosLog[parametro->columna - 1];
                    bufferL.tam++;
                }
            }
        }

        sem_wait(&prod[parametro->nBuf]);
        sem_wait(&mutex[parametro->nBuf]);
        buffers[parametro->nBuf].key = malloc(sizeof(int) * (parametro->hasta - parametro->desde));
        buffers[parametro->nBuf].value = malloc(sizeof(int) * (parametro->hasta - parametro->desde));
        buffers[parametro->nBuf].key = bufferL.key;
        buffers[parametro->nBuf].value = bufferL.value;
        buffers[parametro->nBuf].tam = bufferL.tam;
        sem_post(&mutex[parametro->nBuf]);
        sem_post(&cons[parametro->nBuf]);
    }
};


/*
Función: Reduce.
Precondición: Estructura parametro al igual que la función 'Map'
Postcondición: Se han llenado los outputs y se realiza productor/consumidor con el master.
Sintesis: Cuenta la información de los buffers que producen los mappers.
*/
void Reduce(mparameter *parametro)
{
    static sigset_t mask;

    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR1);
    if (pthread_sigmask(SIG_BLOCK, &mask, NULL) != 0)
    {
        perror("pthread_sigmask");
        exit(1);
    }
    while (true)
    {
        if (pthread_sigmask(SIG_UNBLOCK, &mask, NULL) != 0)
        {
            perror("pthread_sigmask");
            exit(1);
        }
        sem_wait(&master2[parametro->nBufR]);

        int i;
        int auy = 0;
        output[parametro->nBufR] = 0;
        for (i = parametro->desdeR; i < parametro->hastaR; i++)
        {

            sem_wait(&cons[i]);
            sem_wait(&mutex[i]);
            auy += buffers[i].tam;
            sem_post(&mutex[i]);
            sem_post(&prod[i]);
        }
        sem_wait(&Redprod[parametro->nBufR]);
        sem_wait(&Redmutex[parametro->nBufR]);
        output[parametro->nBufR] = auy;
        sem_post(&Redmutex[parametro->nBufR]);
        sem_post(&Redcons[parametro->nBufR]);
    }
};

/*
Función: main.
Precondición: El nombre del archivo de logs, el número de líneas del archivo a leer junto con el número de mappers y reducers.
Postcondición: Muestra el resultado de la consulta junto con el tiempo que se demoro.
Sintesis: Asigna valores a las distintas variables. Hace el llamado a las distintas funciones. Crea los hilos e instala el manejador de la señal.
Menú que permite seleccionar entre 'Realizar consulta' y 'Salir'.
*/
int main(int argc, char const *argv[])
{
    if (argc != 5)
    {
        printf("Parametros incorrectos \n");
        printf("Forma correcta: ./[analogh2] [nombre_del_archivo_de_logs] [cantidad_de_lineas] [numero_de_mappers] [numero_de_reducers]\n");
        exit(1);
    }
    int op;
    int nlineas = atoi(argv[2]);
    int ajuste = 0;
    bool flag_ajuste = false;
    char consulta[50];
    int ncolumna, nsigno, nvalor;
    char *columna;
    char *signo;
    char *valor;
    int nmappers = atoi(argv[3]);
    int nreducers = atoi(argv[4]);
    int columna_validar, intervalo, status;
    mparameter *MapParam[nmappers];
    pthread_t thread1[nmappers];
    pthread_t thread2[nreducers];
    struct timeval start, end;

    int esp;
    int i, j, t, m, y;
    int total_m = 0;
    if (nlineas < 0)
    {
        printf("Numero de registros no validos...\n");
        exit(1);
    }
    if (nreducers > nmappers)
    {
        printf("El número de reducers no debe ser mayor al número de mappers\n");
        exit(1);
    }
    if ((logs = (Log *)malloc(nlineas * sizeof(Log))) == NULL)
    {
        perror("Malloc: ");
        exit(1);
    }
    if ((buffers = (Buf *)malloc(nmappers * sizeof(Buf))) == NULL)
    {
        perror("Malloc Buf: ");
        exit(1);
    }
    leerarchivo(argv[1], nlineas, logs);
    tam_buffer = nmappers;
    iniciarSemaforos(nreducers);
    signal(SIGUSR1, signalHandler);
    for (i = 0; i < nmappers; i++)
    {
        MapParam[i] = (mparameter *)malloc(sizeof(mparameter));
        MapParam[i]->nBuf = i;
        pthread_create(&thread1[i], NULL, (void *)Map, (void *)MapParam[i]);
    }
    for (i = 0; i < nreducers; i++)
    {
        MapParam[i]->nBufR = i;
        pthread_create(&thread2[i], NULL, (void *)Reduce, (void *)MapParam[i]);
    }
    while (op != 2)
    {
        printf("1. Realizar Consulta\n");
        printf("2. Salir del sistema\n");
        scanf("%i", &op);
        switch (op)
        {
        case 1:
        {
            consulta;
            printf("Ingrese su consulta\n");
            scanf("%s", consulta);
            columna = NULL;
            signo = NULL;
            valor = NULL;
            columna = strtok(consulta, ",");
            signo = strtok(NULL, ",");
            valor = strtok(NULL, ",");
            gettimeofday(&start, NULL);
            if (strcmp(signo, "<") != 0)
            {
                if (strcmp(signo, "<=") != 0)
                {
                    if (strcmp(signo, ">") != 0)
                    {
                        if (strcmp(signo, ">=") != 0)
                        {
                            if (strcmp(signo, "=") != 0)
                            {
                                printf("Signo incorrecto...\n");
                                exit(1);
                            }
                            else
                                nsigno = 4;
                        }
                        else
                            nsigno = 3;
                    }
                    else
                        nsigno = 2;
                }
                else
                    nsigno = 1;
            }
            else
                nsigno = 0;
            columna_validar = atoi(columna);
            if (columna_validar < 1)
            {
                printf("Columna no valida...\n");
                exit(1);
            }
            if (columna_validar > 18)
            {
                printf("Columna no valida...\n");
                exit(1);
            }

            ncolumna = atoi(columna);
            nvalor = atoi(valor);
            intervalo = nlineas / nmappers;
            if (intervalo * nmappers != nlineas)
            {
                ajuste = nlineas - (intervalo * nmappers);
                flag_ajuste = true;
            }

            for (i = 0; i < nmappers; i++)
            {
                /*Codigo que ejecutaran los hijos*/
                if ((i == nmappers - 1) && (flag_ajuste == true))
                {
                    MapParam[i]->desde = i * intervalo;
                    MapParam[i]->hasta = ((i + 1) * intervalo + ajuste);
                    MapParam[i]->columna = ncolumna;
                    MapParam[i]->signo = nsigno;
                    MapParam[i]->valor = nvalor;
                    MapParam[i]->nBuf = i;
                    sem_post(&master1[i]);
                }
                else
                {
                    /*MapParam[i] = (mparameter *)malloc(sizeof(mparameter));*/
                    MapParam[i]->desde = i * intervalo;
                    MapParam[i]->hasta = ((i + 1) * intervalo);
                    MapParam[i]->columna = ncolumna;
                    MapParam[i]->signo = nsigno;
                    MapParam[i]->valor = nvalor;
                    MapParam[i]->nBuf = i;
                    sem_post(&master1[i]);
                }
            }

            /*Proceso principal*/
            if ((output = malloc(sizeof(int) * nreducers)) == NULL)
            {
                perror("Malloc output: ");
                exit(1);
            }

            flag_ajuste = false;
            intervalo = nmappers / nreducers;
            if (intervalo * nreducers != nmappers)
            {
                ajuste = nmappers - (intervalo * nreducers);
                flag_ajuste = true;
            }

            for (t = 0; t < nreducers; t++)
            {
                /*Codigo que ejecutaran los hijos*/
                if ((t == nreducers - 1) && (flag_ajuste == true))
                {

                    MapParam[t]->desdeR = t * intervalo;
                    MapParam[t]->hastaR = ((t + 1) * intervalo + ajuste);
                    MapParam[t]->nBufR = t;
                    sem_post(&master2[t]);
                }
                else
                {
                    MapParam[t]->desdeR = t * intervalo;
                    MapParam[t]->hastaR = (t + 1) * intervalo;
                    MapParam[t]->nBufR = t;
                    sem_post(&master2[t]);
                }
            }

            total_m = 0;
            for (i = 0; i < nreducers; i++)
            {
                sem_wait(&Redcons[i]);
                sem_wait(&Redmutex[i]);
                total_m += output[i];
                sem_post(&Redmutex[i]);
                sem_post(&Redprod[i]);
            }
            printf("Total %i\n", total_m);
            /*Proceso principal*/

            printf("Numero de registro que satisfacen la condicion: %i\n", total_m);
            gettimeofday(&end, NULL);
            printf("Tardó :");
            printf("%ld\n", ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec)));
            free(output);
            break;
        }
        case 2:
        {
            matarHilos(thread1, thread2, nmappers, nreducers);
            exit(0);
        }
        default:
        {
            printf("Comando no valido...\n");
            break;
        }
        }
    }
}
