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

#define MAXLIN 150

Log *logs;
Buf *buffers;
int *output;


int mgetline(char *line, int max, FILE *f){
    if (fgets(line, max, f)== NULL)
        return(0);
    else return(strlen(line));
}

void leerarchivo(const char *logfile,int nlineas,Log *logs){
    FILE *archivo;
    char lineas[MAXLIN];
    int i = 0;
    archivo = fopen(logfile, "r");
    if (archivo == NULL) {
        printf("Archivo null....");
        exit(1);
    }
    while (mgetline(lineas, sizeof(lineas), archivo) > 0) {
        sscanf(lineas, "%i %i %i %i %i %i %i %i %i %i %i %i %i %i %i %i %i %i", &logs[i].datosLog[0], &logs[i].datosLog[1], &logs[i].datosLog[2], &logs[i].datosLog[3], &logs[i].datosLog[4], &logs[i].datosLog[5], &logs[i].datosLog[6], &logs[i].datosLog[7], &logs[i].datosLog[8], &logs[i].datosLog[9], &logs[i].datosLog[10], &logs[i].datosLog[11], &logs[i].datosLog[12], &logs[i].datosLog[13], &logs[i].datosLog[14], &logs[i].datosLog[15], &logs[i].datosLog[16], &logs[i].datosLog[17]);
        i++;
    }
    /*printf("%i",logs[4].datosLog[0]);*/
    fclose(archivo);
}

void Map(mparameter *parametro){
    int i=0;
    buffers[parametro->nBuf].key=malloc(sizeof(int)*(parametro->hasta-parametro->desde));
    buffers[parametro->nBuf].value=malloc(sizeof(int)*(parametro->hasta-parametro->desde));
    buffers[parametro->nBuf].tam=0;

    for(i=parametro->desde;i<parametro->hasta;i++){
        if(parametro->signo==0){
            if(logs[i].datosLog[parametro->columna-1]<parametro->valor){
                buffers[parametro->nBuf].key[buffers[parametro->nBuf].tam]=logs[i].datosLog[0];
                buffers[parametro->nBuf].value[buffers[parametro->nBuf].tam]=logs[i].datosLog[parametro->columna-1];
                buffers[parametro->nBuf].tam++;
            }
        }
        if(parametro->signo==1){
            if(logs[i].datosLog[parametro->columna-1]<=parametro->valor){
              buffers[parametro->nBuf].key[buffers[parametro->nBuf].tam]=logs[i].datosLog[0];
              buffers[parametro->nBuf].value[buffers[parametro->nBuf].tam]=logs[i].datosLog[parametro->columna-1];
              buffers[parametro->nBuf].tam++;
            }
        }
        if(parametro->signo==2){
            if(logs[i].datosLog[parametro->columna-1]>parametro->valor){
              buffers[parametro->nBuf].key[buffers[parametro->nBuf].tam]=logs[i].datosLog[0];
              buffers[parametro->nBuf].value[buffers[parametro->nBuf].tam]=logs[i].datosLog[parametro->columna-1];
              buffers[parametro->nBuf].tam++;
            }
        }
        if(parametro->signo==3){
            if(logs[i].datosLog[parametro->columna-1]>=parametro->valor){
              buffers[parametro->nBuf].key[buffers[parametro->nBuf].tam]=logs[i].datosLog[0];
              buffers[parametro->nBuf].value[buffers[parametro->nBuf].tam]=logs[i].datosLog[parametro->columna-1];
              buffers[parametro->nBuf].tam++;
            }
        }
        if(parametro->signo==4) {
            if (logs[i].datosLog[parametro->columna - 1] == parametro->valor) {
              buffers[parametro->nBuf].key[buffers[parametro->nBuf].tam]=logs[i].datosLog[0];
              buffers[parametro->nBuf].value[buffers[parametro->nBuf].tam]=logs[i].datosLog[parametro->columna-1];
              buffers[parametro->nBuf].tam++;
            }
        }
    }
    pthread_exit(NULL);
};

void Reduce(mparameter *parametro){
    int i;
    output[parametro->nBuf] = 0;
    for (i = parametro->desde; i < parametro->hasta; i++) {
            output[parametro->nBuf] += buffers[i].tam;
    }
    pthread_exit(NULL);
};

int main(int argc, char const *argv[]) {
    if (argc != 5) {
        printf("Parametros incorrectos \n");
        exit(1);
    }
    int op;
    int nlineas=atoi(argv[2]);
    int ajuste=0;
    bool flag_ajuste=false;
    char consulta[50];
    int ncolumna;
    int nsigno;
    int nvalor;
    char *columna;
    char *signo;
    char *valor;
    int nmappers=atoi(argv[3]);
    int nreducers=atoi(argv[4]);
    int columna_validar;
    int intervalo;
    int status;
    int childpid=0;
    struct timeval start,end;
    int i;
    int j;
    int t;
    int m;
    int y;
    int total_m = 0;
    if(nlineas<0){
        printf("Numero de registros no validos...\n");
        exit(1);
    }
    if((logs=(Log *)malloc (nlineas* sizeof(Log)))==NULL){
        perror("Malloc: ");
        exit(1);
    }
    if((buffers=(Buf *)malloc (nmappers* sizeof(Buf)))==NULL){
        perror("Malloc Buf: ");
        exit(1);
    }
    leerarchivo(argv[1],nlineas,logs);
    while(op!=2){
        printf("1. Realizar Consulta\n" );
        printf("2. Salir del sistema\n" );
        scanf("%i", &op );
        switch (op) {
            case 1: {
                consulta;
                printf("Ingrese su consulta\n");
                scanf("%s",consulta);
                columna=NULL;
                signo=NULL;
                valor=NULL;
                columna=strtok(consulta,",");
                signo=strtok(NULL,",");
                valor=strtok(NULL,",");
                gettimeofday(&start,NULL);
                if(strcmp(signo,"<")!=0){
                    if(strcmp(signo,"<=")!=0){
                        if(strcmp(signo,">")!=0){
                            if(strcmp(signo,">=")!=0){
                                if(strcmp(signo,"=")!=0){
                                    printf("Signo incorrecto...\n");
                                    exit(1);
                                } else
                                    nsigno=4;
                            } else
                                nsigno=3;
                        } else
                            nsigno=2;
                    } else
                        nsigno=1;
                } else
                    nsigno=0;
                columna_validar=atoi(columna);
                if(columna_validar<1){
                    printf("Columna no valida...\n");
                    exit(1);
                }
                if(columna_validar>18){
                    printf("Columna no valida...\n");
                    exit(1);
                }
                ncolumna=atoi(columna);
                nvalor=atoi(valor);
                intervalo=nlineas/nmappers;
                if(intervalo*nmappers!=nlineas){
                    ajuste=nlineas-(intervalo*nmappers);
                    flag_ajuste=true;
                }
		    mparameter *MapParam[nmappers];
		    pthread_t thread1[nmappers];
                    for(i=0;i<nmappers;i++){

                    /*Codigo que ejecutaran los hijos*/
                    if((i==nmappers-1)&&(flag_ajuste==true)){
			            MapParam[i] = (mparameter *) malloc(sizeof(mparameter));
                        MapParam[i]->desde=i*intervalo;
                        MapParam[i]->hasta=((i+1)*intervalo+ajuste);
                        MapParam[i]->columna=ncolumna;
                        MapParam[i]->signo=nsigno;
                        MapParam[i]->valor=nvalor;
                        MapParam[i]->nBuf=i;
                        pthread_create(&thread1[i],NULL,(void*)Map,(void*)MapParam[i]);
                    }
                    else{

			            MapParam[i] = (mparameter *) malloc(sizeof(mparameter));
                        MapParam[i]->desde=i*intervalo;
                        MapParam[i]->hasta=((i+1)*intervalo);
                        MapParam[i]->columna=ncolumna;
                        MapParam[i]->signo=nsigno;
                        MapParam[i]->valor=nvalor;
                        MapParam[i]->nBuf=i;
                        pthread_create(&thread1[i],NULL,(void*)Map,(void*)MapParam[i]);
                        }
                    }
		            int esp;
		            for(esp=0;esp<nmappers;esp++){
			        pthread_join(thread1[esp],NULL);
		            }
		            /*Proceso principal*/
		            if((output=malloc(sizeof(int)*nreducers))==NULL){
		                perror("Malloc output: ");
			            exit(1);
		            }

                    flag_ajuste=false;
                    intervalo=nmappers/nreducers;
                    if(intervalo*nreducers!=nmappers){
                        ajuste=nmappers-(intervalo*nreducers);
                        flag_ajuste=true;
                    }
                pthread_t thread2[nreducers];
                    for(t=0;t<nreducers;t++){
                        /*Codigo que ejecutaran los hijos*/
                        if((t==nreducers-1)&&(flag_ajuste==true)){
                            MapParam[t]->desde = t*intervalo;
                            MapParam[t]->hasta = ((t+1)*intervalo + ajuste);
                            MapParam[t]->nBuf = t;
                            pthread_create(&thread2[t],NULL,(void*)Reduce,(void*)MapParam[t]);
                    }
                    else{
                        MapParam[t]->desde = t*intervalo;
                        MapParam[t]->hasta = (t+1)*intervalo;
                        MapParam[t]->nBuf = t;
                        pthread_create(&thread2[t],NULL,(void*)Reduce,(void*)MapParam[t]);
                    }

                }
		for(esp=0;esp<nreducers;esp++){
			pthread_join(thread2[esp],NULL);
		}
                total_m=0;
                for ( i = 0; i < nreducers; i++) {
                    total_m += output[i];
                }
                printf("Total %i\n", total_m );
                /*Proceso principal*/

                printf("Numero de registro que satisfacen la condicion: %i\n",total_m);
                gettimeofday(&end,NULL);
                printf("Tardó :");
                printf("%ld\n",((end.tv_sec*1000000+end.tv_usec)-(start.tv_sec*1000000+start.tv_usec)));
                free(output);
                break;
            }
            case 2:{
                free(logs);
                free(buffers);
                exit(0);

            }
            default :{
                printf("Comando no valido...\n");
                break;
            }
        }
    }

}
