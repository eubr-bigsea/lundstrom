//====================================================== file = makva.c =====
//=  Approximate Mean Value Analysis for closed queueing networks           =
//===========================================================================
//=  Solves a closed queuing network through modified MVA according to      =
//=  Mak & Lundstrom 1990 and Liang & Tripathi 2000                         =
//=                                                                         =
//=  Input metrics are N (number of tasks), C (number of service centers),  =
//=  epsilon (error tolerance), average response time for each task,        =
//=  service demand for each task and  the matrix of task overlaps,         =
//=  which should be calculated from the task graph in a previous step.     =
//=                                                                         =
//=  Output metric is the total job response time.                          =
//=  Sample output:                                                         =
//=  R = 2.584088                                                           =
//=-------------------------------------------------------------------------=
//=  Build:  gcc makva.c -o makva                                           =
//=-------------------------------------------------------------------------=
//=  Run:  ./makva -?                                                       =
//===========================================================================

#include <stdio.h> 
#include <stdlib.h>
#include <unistd.h>

void init(double** A, int m, int n){
    int i, j;
 
    for(i=0; i<m; i++){
        for(j=0; j<n; j++){
            A[i][j] = 0;
        }
    }
}

double** allocate(int m, int n){
    double **A;
    int i;
 
    A = (double **) calloc (m, sizeof(double *));
    for (i=0; i<m; i++) {
          A[i] = (double*) calloc (n, sizeof(double));
    }
 
    init(A, m, n);
    return (A);
}
 
void unallocate(double** A, int m){
    int i;
 
    if (A==NULL) return; 
    for (i=0; i<m; i++) free(A[i]);
    free(A); 
}

void usage(){
  fprintf(stderr, "Usage:\n\
  -N Number of tasks\n\
  -C Number of servers\n\
  -e Error tolerance\n\
  -r Log file containing response times for each task\n\
  -s Log file containing service demands for each task\n\
  -o File containing the task overlap matrix\
  \n\
  Format:\n\
  ./mva_single -N <num tasks> -C <num centers> -e <error>\
  -r <log.txt> -s <log.txt> -o <log.txt>\n\
  \n\
  Example:\n\
  ./makva -N 4 -C 1 -e 2.5 -r response.txt -s demand.txt -o overlap.txt\n");
}

void main(int argc, char *argv[]){
  FILE *response, *demand, *overlap;
  int N, C;               // Number of tasks, number of centers
  int i, j, k;            // Tasks, centers
  double **theta;         // Task overlap
  double **R, **D, **Q, **A;  // Task response times, service demands, queue length, arrival queue length
  double R_l, R_s;        // Response time of the system
  double eps;             // Error tolerance
  double s;
  int opt, converged;
  
  // Get server info  
  while((opt=getopt(argc,argv,"N:C:e:r:s:o:"))!=-1){
    switch(opt){
      case 'N':
        N = atoi(optarg); break;
      case 'C':
	C = atoi(optarg); break;
      case 'e':
        eps = atof(optarg); break;
      case 'r':
        response = fopen(optarg, "r"); break;
      case 's':
        demand = fopen(optarg, "r"); break;
      case 'o':
        overlap = fopen(optarg, "r"); break;
      default:
        case '?': usage();
        return;
    }
  }
  theta = allocate(N, N);
  R = allocate(N, C);
  D = allocate(N, C);
  Q = allocate(N, C);
  A = allocate(N, C);

  
  // Get initial response time values
  R_l = 0;
  R_s = 0;
  for(i=0; i<N; i++){
    for(k=0; k<C; k++){
      fscanf(response, "%lf", &R[i][k]);
      R_s += R[i][k];
    }
  }

  // Get service demand values
  for(i=0; i<N; i++){
    for(k=0; k<C; k++){
      fscanf(demand, "%lf", &D[i][k]);
    }
  }
  
  // Calculate overlap - not implemented, input values instead
  for(i=0; i<N; i++){
    for(j=0; j<N; j++){
      //theta[i][j] = overlap();
      fscanf(overlap, "%lf", &theta[i][j]);
    }
  }
  
  converged = 0;
  while(converged==0){
    
    // Calculate reduced population residence times 
    for(j=0; j<N; j++){
      for(i=0; i<N; i++){
	s = 0;
        for(k=0; k<C; k++){
          s += R[i][k];
        }
	for(k=0; k<C; k++){
	  R[j][k] = R[j][k] - theta[j][i]/N * D[j][k]*R[i][k]/s;
	}
      }      
    }
    
    // Calculate queue length
    for(j=0; j<N; j++){
      s = 0;
      for(k=0; k<C; k++){
	s += R[j][k];
      }
      for(k=0; k<C; k++){
	Q[j][k] = R[j][k]/s;
      }
    }
    
    // Calculate queue length at time of arrival
    for(i=0; i<N; i++){
      for(k=0; k<C; k++){
        s = 0;
        for(j=0; j<N; j++){
          s += theta[i][j] * Q[j][k];
        }
	A[i][k] = s;
      }
    }
    
    // Calculate response time in each center
    for(i=0; i<N; i++){
      for(k=0; k<C; k++){
	R[i][k] = D[i][k] * (1 + A[i][k]);
      }
    }
    
    // Calculate total response time of the job
    for(i=0; i<N; i++){
      for(k=0; k<C; k++){
	R_l += R[i][k];
      }
    }
    
    // Convergence test
    if(abs(R_s - R_l) < eps){ 
      converged = 1;
    } else {
      R_s = R_l;
      R_l = 0.0;
    }
  }
  
  printf("R: %f\n", R_l);
}
