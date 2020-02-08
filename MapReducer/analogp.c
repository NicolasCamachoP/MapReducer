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

#define MAXLIN 300


int mgetline(char *line, int max, FILE *f){
    if (fgets(line, max, f)== NULL)
        return(0);
    else return(strlen(line));
}

void leerarchivo(const char *logfile,int nlineas,Log *log){
    FILE *archivo;
    char lineas[MAXLIN];
    int i = 0;
    archivo = fopen(logfile, "r");
    if (archivo == NULL) {
        printf("Archivo null....");
        exit(1);
    }
    while (mgetline(lineas, sizeof(lineas), archivo) > 0) {
        sscanf(lineas, "%i %i %i %i %i %i %i %i %i %i %i %i %i %i %i %i %i %i", &log[i].datosLog[0], &log[i].datosLog[1], &log[i].datosLog[2], &log[i].datosLog[3], &log[i].datosLog[4], &log[i].datosLog[5], &log[i].datosLog[6], &log[i].datosLog[7], &log[i].datosLog[8], &log[i].datosLog[9], &log[i].datosLog[10], &log[i].datosLog[11], &log[i].datosLog[12], &log[i].datosLog[13], &log[i].datosLog[14], &log[i].datosLog[15], &log[i].datosLog[16], &log[i].datosLog[17]);
        i++;
    }
    /*printf("%i",log[4].datosLog[0]);*/
    fclose(archivo);
}

void Map(Log *log,int desde,int hasta,char *columna,char *signo,char* valor,bool borrar,int c){
    /*printf("Soy el hijo con PID %d\n",getpid());*/
    /*printf("Soy el hijo con c %d\n",c);*/
    int ncolumna=atoi(columna);
    int nvalor=atoi(valor);
    char nc[10];
    sprintf(nc,"%i",c);
    strcat(nc,".buf");
    /*printf("Nombre de archivo %s",nombre);*/
    FILE *buf_manager=NULL;
    buf_manager=fopen(nc,"w");
    char aux_k[4];
    char aux_v[28];
    int i;
    if(buf_manager==NULL){
        perror("fopen: ");
        exit(1);
    }
    for(i=desde;i<hasta;i++){
        if(strcmp(signo,"<")==0){
            if(log[i].datosLog[ncolumna-1]<nvalor){
                sprintf(aux_k,"%i",log[i].datosLog[0]);
                sprintf(aux_v,"%i",log[i].datosLog[ncolumna-1]);
                strcat(aux_k,",");
                strcat(aux_k,aux_v);
                strcat(aux_k,"\n");
                fputs(aux_k,buf_manager);
            }
        }
        if(strcmp(signo,"<=")==0){
            if(log[i].datosLog[ncolumna-1]<=nvalor){
                sprintf(aux_k,"%i",log[i].datosLog[0]);
                sprintf(aux_v,"%i",log[i].datosLog[ncolumna-1]);
                strcat(aux_k,",");
                strcat(aux_k,aux_v);
                strcat(aux_k,"\n");
                fputs(aux_k,buf_manager);
            }
        }
        if(strcmp(signo,">")==0){
            if(log[i].datosLog[ncolumna-1]>nvalor){
                sprintf(aux_k,"%i",log[i].datosLog[0]);
                sprintf(aux_v,"%i",log[i].datosLog[ncolumna-1]);
                strcat(aux_k,",");
                strcat(aux_k,aux_v);
                strcat(aux_k,"\n");
                fputs(aux_k,buf_manager);
            }
        }
        if(strcmp(signo,">=")==0){
            if(log[i].datosLog[ncolumna-1]>=nvalor){
                sprintf(aux_k,"%i",log[i].datosLog[0]);
                sprintf(aux_v,"%i",log[i].datosLog[ncolumna-1]);
                strcat(aux_k,",");
                strcat(aux_k,aux_v);
                strcat(aux_k,"\n");
                fputs(aux_k,buf_manager);
            }
        }
        if(strcmp(signo,"=")==0) {
            if (log[i].datosLog[ncolumna - 1] == nvalor) {
                sprintf(aux_k, "%i", log[i].datosLog[0]);
                sprintf(aux_v, "%i", log[i].datosLog[ncolumna - 1]);
                strcat(aux_k, ",");
                strcat(aux_k, aux_v);
                strcat(aux_k, "\n");
                fputs(aux_k, buf_manager);
            }
        }
    }
    fclose(buf_manager);
};

void Reduce(int bufs[],int desde,int hasta,char *columna,char *signo,char* valor,bool borrar,int c){
/*printf("Soy el hijo con PID %d\n",getpid());*/
/*printf("Soy el hijo con c %d\n",c);*/
    char nc[10];
    sprintf(nc,"%i",c);
    strcat(nc,".out");
    FILE *output_manager=NULL;
    output_manager=fopen(nc,"w");
    char buf_fname[10];
    FILE *buf_manager=NULL;
    int total=0;
    char totalC[10];
    int buf_reader;
    int rmve;
    int i;
    if(output_manager==NULL){
        perror("fopen: ");
        exit(1);
    }
    for(i=desde;i<hasta;i++){
        /*printf("Iteracion en PID %i numero %i\n",getpid(),i);*/
        buf_manager=NULL;
        sprintf(buf_fname,"%i",bufs[i]);
        strcat(buf_fname,".buf");
        buf_manager=fopen(buf_fname,"r");
        if(buf_manager==NULL){
            perror("fopen: ");
            exit(1);
        }
        while((buf_reader=fgetc(buf_manager))!=EOF){
            if(buf_reader=='\n')
                total++;
        }
        fclose(buf_manager);
        if(borrar==true){
            rmve=remove(buf_fname);
            if(rmve!=0){
                perror("Remove: ");
                exit(1);
            }
        }
        buf_manager=NULL;
    }
    sprintf(totalC,"%i",total);
    fputs(totalC,output_manager);
    fclose(output_manager);
};

int main(int argc, char const *argv[]) {
    if (argc != 6) {
        printf("Parametros incorrectos \n");
        exit(1);
    }
    int op;
    int nlineas=atoi(argv[2]);
    int ajuste=0;
    bool flag_ajuste=false;
    char consulta[50];
    char *columna;
    char *signo;
    char *valor;
    Log *log;
    int nmappers=atoi(argv[3]);
    int nreducers=atoi(argv[4]);
    int intermedios=atoi(argv[5]);
    int columna_validar;
    int intervalo;
    int status;
    int childpid=0;
    int bufs[nmappers];
    int outs[nreducers];
    bool borrar=false;
    FILE *outs_manager=NULL;
    char output_fname[10];
    char readed[15];
    int TOTAL=0;
    int aux_outsread=0;
    int rmve;
    struct timeval start,end;
    int i;
    int j;
    int t;
    int m;
    int y;
    if(intermedios==0)
        borrar=true;
    if((log = (Log *)malloc (nlineas* sizeof(Log)))==NULL){
        perror("Malloc: ");
        exit(1);
    }
    leerarchivo(argv[1],nlineas,log);
    /*printf("%i",log[0].datosLog[0]);*/
    while(op!=2){
        printf("1. Realizar Consulta\n" );
        printf("2. Salir del sistema\n" );
        scanf("%i", &op );
        switch (op) {
            case 1: {
                printf("Ingrese su consulta\n");
                scanf("%s",consulta);
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
                                }
                            }
                        }
                    }
                }
                columna_validar=atoi(columna);
                if(columna_validar<1){
                    printf("Columna no valida...\n");
                    exit(1);
                }
                if(columna_validar>18){
                    printf("Columna no valida...\n");
                    exit(1);
                }
                intervalo=nlineas/nmappers;
                if(intervalo*nmappers!=nlineas){
                    ajuste=nlineas-(intervalo*nmappers);
                    flag_ajuste=true;
                }

                for(i=0;i<nmappers;i++){
                    if((childpid=fork())<0){
                        perror("fork: ");
                        exit(1);
                    }
                    /*Codigo que ejecutaran los hijos*/
                    if(childpid==0){
                        if((i==nmappers-1)&&(flag_ajuste==true)){
                            Map(log,i*intervalo,((i+1)*intervalo+ajuste),columna,signo,valor,borrar,i);
                            exit(0);
                        }
                        else{
                            Map(log,i*intervalo,(i+1)*intervalo,columna,signo,valor,borrar,i);
                            exit(0);
                        }
                    }
                }

                for(j=0;j<nmappers;j++){
                    wait(&status);
                    bufs[j]=j;
                }
                /*Proceso principal*/

                flag_ajuste=false;
                intervalo=nmappers/nreducers;
                if(intervalo*nreducers!=nmappers){
                    ajuste=nmappers-(intervalo*nreducers);
                    flag_ajuste=true;
                }
                for(t=0;t<nreducers;t++){
                    if((childpid=fork())<0){
                        perror("fork: ");
                        exit(1);
                    }
                    /*Codigo que ejecutaran los hijos*/
                    if(childpid==0){
                        if((t==nreducers-1)&&(flag_ajuste==true)){
                            Reduce(bufs,t*intervalo,((t+1)*intervalo+ajuste),columna,signo,valor,borrar,t);
                            exit(0);
                        }
                        else{
                            Reduce(bufs,t*intervalo,(t+1)*intervalo,columna,signo,valor,borrar,t);
                            exit(0);
                        }
                    }
                }
                for(m=0;m<nreducers;m++){
                    wait(&status);
                    outs[m]=m;
                }
                /*Proceso principal*/
                for(y=0;y<nreducers;y++) {
                    sprintf(output_fname, "%i", y);
                    strcat(output_fname, ".out");
                    outs_manager = fopen(output_fname, "r");
                    if (outs_manager == NULL) {
                        perror("fopen: ");
                        exit(1);
                    }
                    if (fgets(readed, 15, outs_manager) != NULL) {
                        aux_outsread = atoi(readed);
                        TOTAL += aux_outsread;
                    }
                    fclose(outs_manager);
                    if (borrar == true) {
                        rmve = remove(output_fname);
                        if (rmve != 0) {
                            perror("Remove: ");
                            exit(1);
                        }
                    }
                    outs_manager = NULL;
                }
                printf("Numero de registro que satisfacen la condicion: %i\n",TOTAL);
                TOTAL=0;
                gettimeofday(&end,NULL);
                printf("Tardó :");
                printf("%ld\n",((end.tv_sec*1000000+end.tv_usec)-(start.tv_sec*1000000+start.tv_usec)));
		        break;
            }
            case 2: {
                free(log);
                exit(0);
                break;
            }
            default:{
		        printf("Comando no valido...\n");
                break;
	    }

        }
    }
}
